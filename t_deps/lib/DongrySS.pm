package DongrySS;
use strict;
use warnings;
use Path::Tiny;
use Promise;
use ServerSet;

my $RootPath = path (__FILE__)->parent->parent->parent->absolute;

sub run ($%) {
  ## Arguments:
  ##   signal         AbortSignal canceling the server set.  Optional.
  my $class = shift;
  return ServerSet->run ({
    proxy => {
      start => sub {
        return [undef, undef];
      },
    },
    mysqld => {
      handler => 'ServerSet::MySQLServerHandler',
    },
    _ => {
      requires => ['mysqld'],
      start => sub {
        my ($handler, $self, %args) = @_;
        return Promise->all ([
          $args{receive_mysqld_data},
        ])->then (sub {
          my ($mysqld_data) = @{$_[0]};

          my $data = {};
          $data->{local_dsn_options} = $mysqld_data->{local_dsn_options};
          $data->{local_dsn_options}->{root} = {%{$mysqld_data->{local_dsn_options}->{test}}};
          $data->{local_dsn_options}->{root}->{user} = 'root';
          $data->{local_dsn_options}->{root}->{password} = $self->key ('mysqld_root_password');

          $data->{mysql_version} = $mysqld_data->{mysql_version};
          
          return [$data, undef];
        });
      },
    }, # _
  }, sub {
    my ($ss, $args) = @_;
    my $result = {};

    $result->{server_params} = {
      proxy => {
      },
      mysqld => {
        databases => {
        },
        no_dump => 1,
        database_name_suffix => $args->{mysqld_database_name_suffix},
        volume_path => $args->{path},
        mycnf => $args->{mycnf},
        mysql_version => $args->{mysql_version},
        old_sql_mode => $args->{old_sql_mode},
        socket => 1,
      },
      _ => {},
    }; # $result->{server_params}

    return $result;
  }, @_);
} # run

1;

=head1 LICENSE

Copyright 2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
