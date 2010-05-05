package Perlbal::Plugin::StickySessions;

use Perlbal;
use strict;
use warnings;
use Data::Dumper;
use HTTP::Date;
use CGI qw/:standard/;
use CGI::Cookie;
use Scalar::Util qw(blessed reftype);


# LOAD StickySessions
# SET plugins        = stickysessions
#
# Add 
#    my $svc = $self->{service};
#    if(ref($svc) && UNIVERSAL::can($svc,'can')) {
#      $svc->run_hook('modify_response_headers', $self);
#    }
# To sub handle_response in BackendHTTP after Content-Length is set.
#

sub load {
    my $class = shift;
    return 1;
}

sub unload {
    my $class = shift;
    return 1;
}

sub get_backend_id {
    my $be = shift;

    for ( my $i = 0 ; $i <= $#{ $be->{ service }->{ pool }->{ nodes } } ; $i++ )
    {
        my ( $nip, $nport ) = @{ $be->{ service }->{ pool }->{ nodes }[$i] };
        my $nipport = $nip . ':' . $nport;
        return $i + 1 if ( $nipport eq $be->{ ipport } );
    }

    # default to the first backend in the node list.
    return 1;
}

sub decode_server_id {
    my ($svc,$id) = @_;
    
    my $ret = 0;
    eval {
      $ret = ( $id - 1 ) unless ( ($id-1) > $#{$svc->{pool}->{nodes}} );
    };
    return $ret;
}

sub get_ipport {
    my ( $svc, $req ) = @_;
    my $cookie  = $req->header('Cookie');
    my %cookies = ();
    my $ipport  = undef;

    %cookies = parse CGI::Cookie($cookie) if defined $cookie;
    if ( defined $cookie && defined $cookies{ 'X-SERVERID' } ) {
        my $val =
          $svc->{ pool }
          ->{ nodes }[ decode_server_id($svc, $cookies{ 'X-SERVERID' }->value ) ];
        my ( $ip, $port ) = @{ $val } if defined $val;
        $ipport = $ip . ':' . $port;
    }
    return $ipport;
}

sub check_bored_backend {
    my ( $svc, $req, $client ) = @_;

    my Perlbal::BackendHTTP $be;
    my $ipport = get_ipport( $svc, $req );

    my $tries = 3;
    
    while($tries > 0) {
      $tries--;
      my $now = time;
      while ( $be = shift @{ $svc->{ bored_backends } } ) {
          next if $be->{ closed };

          # now make sure that it's still in our pool, and if not, close it
          next unless $svc->verify_generation($be);

          # don't use connect-ahead connections when we haven't
          # verified we have their attention
          if ( !$be->{ has_attention } && $be->{ create_time } < $now - 5 ) {
              $be->close("too_old_bored");
              next;
          }

          # don't use keep-alive connections if we know the server's
          # just about to kill the connection for being idle
          if ( $be->{ disconnect_at } && $now + 2 > $be->{ disconnect_at } ) {
              $be->close("too_close_disconnect");
              next;
          }

          # give the backend this client
          if ( defined $ipport ) {
              next unless ( $be->{ ipport } eq $ipport );
              if ( $be->assign_client($client) ) {
                $svc->spawn_backends;
                return 1;
              }
          } else {
              if ( $be->assign_client($client) ) {
                  $svc->spawn_backends;
                  return 1;
              }
          }

          # assign client can end up closing the connection, so check for that
          return 1 if $client->{ closed };
      }
      $svc->spawn_backends;
    }

    return 0;
}


# called when we're being added to a service
sub register {
    my ( $class, $gsvc ) = @_;
    
    # Keep a horde of backends up.
    my $cb_spawn;
    $cb_spawn = sub {
      my $now = time;
      while ( my $be = shift @{ $gsvc->{ bored_backends } } ) {
        next if $be->{ closed };

        next unless $gsvc->verify_generation($be);

        if ( !$be->{ has_attention } && $be->{ create_time } < $now - 5 ) {
            $be->close("too_old_bored");
            next;
        }

        if ( $be->{ disconnect_at } && $now + 2 > $be->{ disconnect_at } ) {
            $be->close("too_close_disconnect");
            next;
        }
      }
      $gsvc->spawn_backends;
      Danga::Socket->AddTimer(3, $cb_spawn);
    };
    $cb_spawn->();

    my $check_cookie_hook = sub {
        my Perlbal::ClientProxy $client = shift;
        my Perlbal::HTTPHeaders $req    = $client->{ req_headers };
        return 0 unless defined $req;

        my $svc = $client->{ service };

        # we define were to send the client request
        $client->{ backend_requested } = 1;

        $client->state('wait_backend');

        return unless $client && !$client->{ closed };
        
        $svc->spawn_backends;
        
        if ( check_bored_backend( $svc, $req, $client ) != 1 ) {
          #Perlbal::log('stats','Failed, going to spawn more backends');
          push @{ $svc->{ waiting_clients } }, $client;
          $svc->{ waiting_client_count }++;
          $svc->{ waiting_client_map }{ $client->{ fd } } = 1;
          $svc->spawn_backends;
          $client->tcp_cork(1);
        }

        return 1;
    };

    my $set_cookie_hook = sub {
        my Perlbal::BackendHTTP $be  = shift;
        my Perlbal::HTTPHeaders $hds = $be->{ res_headers };
        my Perlbal::HTTPHeaders $req = $be->{ req_headers };
        return 0 unless defined $be && defined $hds;

        my $svc = $be->{ service };

        my $cookie  = $req->header('Cookie');
        my %cookies = ();
        %cookies = parse CGI::Cookie($cookie) if defined $cookie;

        my $backend_id = get_backend_id($be);
        
        if ( !defined( $cookies{ 'X-SERVERID' } )
            || $cookies{ 'X-SERVERID' }->value != $backend_id )
        {
            my $backend_cookie =
              new CGI::Cookie( -name => 'X-SERVERID', -value => $backend_id );
            if ( defined $hds->header('set-cookie') ) {
                my $val = $hds->header('set-cookie');
                $hds->header( 'Set-Cookie',
                    $val .= "\r\nSet-Cookie: " . $backend_cookie->as_string );
            } else {
                $hds->header( 'Set-Cookie', $backend_cookie );
            }
        }

        return 0;
    };
    
    my $spawn_backends = sub {
        my Perlbal::Service $self = shift;
      
        # check our lock and set it if we can
        return if $self->{spawn_lock};
        $self->{spawn_lock} = 1;

        # sanity checks on our bookkeeping
        if ($self->{pending_connect_count} < 0) {
            Perlbal::log('crit', "Bogus: service $self->{name} has pending connect ".
                         "count of $self->{pending_connect_count}?!  Resetting.");
            $self->{pending_connect_count} = scalar
                map { $_ && ! $_->{closed} } values %{$self->{pending_connects}};
        }

        # keep track of the sum of existing_bored + bored_created
        my $backends_created = scalar(@{$self->{bored_backends}}) + $self->{pending_connect_count};
        my $backends_needed = $self->{waiting_client_count} + $self->{connect_ahead};
        my $to_create = $backends_needed - $backends_created;

        my $pool = $self->{pool};

        # can't create more than this, assuming one pending connect per node
        my $max_creatable = $pool ? ($self->{pool}->node_count - $self->{pending_connect_count}) : 1;
        $to_create = $max_creatable if $to_create > $max_creatable;
        
        if ($pool) {
           if ($backends_created < $self->{pool}->node_count * 3) {
             $to_create = $self->{pool}->node_count * 3
          }
        }

        # We are not worried about the limit since we need connections pooled.
        #$to_create = 10 if $to_create > 10;

        my $now = time;

        while($to_create > 0) {
          foreach my $node (@{$pool->{nodes}}) {
              last if $to_create == 0;
              $to_create--;

              # spawn processes if not a pool, else whine.
              unless ($pool) {
                  if (my $sp = $self->{server_process}) {
                      warn "To create = $to_create...\n";
                      warn "  spawning $sp\n";
                      my $be = Perlbal::BackendHTTP->new_process($self, $sp);
                      return;
                  }
                  warn "No pool! Can't spawn backends.\n";
                  return;
              }

              #my ($ip, $port) = $self->{pool}->get_backend_endpoint;
              my $ip = $node->[0];
              my $port = $node->[1];

              unless ($ip) {
                  Perlbal::log('crit', "No backend IP for service $self->{name}");
                  # FIXME: register desperate flag, so load-balancer module can callback when it has a node
                  $self->{spawn_lock} = 0;
                  return;
              }

              # handle retry timeouts so we don't spin
              next if $self->{backend_no_spawn}->{"$ip:$port"};

              # if it's pending, verify the pending one is still valid
              if (my Perlbal::BackendHTTP $be = $self->{pending_connects}{"$ip:$port"}) {
                  my $age = $now - $be->{create_time};
                  if ($age >= 5 && $be->{state} eq "connecting") {
                      $be->close('connect_timeout');
                  } elsif ($age >= 60 && $be->{state} eq "verifying_backend") {
                      # after 60 seconds of attempting to verify, we're probably already dead
                      $be->close('verify_timeout');
                  } elsif (! $be->{closed}) {
                      next;
                  }
              }

              # now actually spawn a backend and add it to our pending list
              if (my $be = Perlbal::BackendHTTP->new($self, $ip, $port, { pool => $self->{pool} })) {
                  $self->add_pending_connect($be);
              }
          }
        }

        # clear our spawn lock
        $self->{spawn_lock} = 0;
      
        return 1;
    };
    
    # Add to sub spawn_backends {} to overload it.
    #return if $self->run_hook('spawn_backends', $self);
    $gsvc->register_hook( 'StickySessions', 'spawn_backends', $spawn_backends );
    
    $gsvc->register_hook( 'StickySessions', 'start_proxy_request', $check_cookie_hook );
    $gsvc->register_hook( 'StickySessions', 'modify_response_headers', $set_cookie_hook );
    return 1;
}

# called when we're no longer active on a service
sub unregister {
    my ( $class, $svc ) = @_;
    $svc->unregister_hooks('StickySessions');
    $svc->unregister_setters('StickySessions');
    return 1;
}

1;

=head1 NAME

Perlbal::Plugin::StickySessions - session affinity for perlbal

=head1 SYNOPSIS

This plugin provides a Perlbal the ability to load balance with 
session affinity.

You *must* patch Perlbal for this plugin to work correctly.

Configuration as follows:

  LOAD StickySessions
  SET plugins        = stickysessions

=cut