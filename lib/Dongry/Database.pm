package Dongry::Database;
use strict;
use warnings;
our $VERSION = '6.0';
use Carp;
use Carp::Heavy;
use Scalar::Util qw(weaken);

use Dongry::SQL ();
BEGIN {
  *_quote = \&Dongry::SQL::quote;
  *_fields = \&Dongry::SQL::fields;
  *_where = \&Dongry::SQL::where;
  *_order = \&Dongry::SQL::order;
}

push our @CARP_NOT, qw(
  DBI DBI::st DBI::db
  Dongry::Database::Executed Dongry::Database::Executed::Inserted
  Dongry::Database::Transaction Dongry::Database::AETransaction
  Dongry::Database::ForceSource
  Dongry::Table Dongry::Table::Row Dongry::Query
  Dongry::SQL
  Dongry::Database::BrokenConnection
  AnyEvent::MySQL::Client
  AnyEvent::MySQL::Client::Promise
);

our $ListClass ||= 'List::Ish';

sub _list {
  eval qq{ require $ListClass } or die $@;
  return $ListClass->new ($_[1] || []);
} # _list

our $SQLDebugClass ||= 'DBIx::ShowSQL';
our $AESQLDebugClass ||= 'AnyEvent::MySQL::Client::ShowLog';
our $SQL_DEBUG ||= $ENV{SQL_DEBUG};

# ------ Construction ------

sub new ($;%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

our $Registry ||= {};
our $Instances = {};
END { $Instances = {} }

sub load ($$) {
  return $Instances->{$_[1]} if $Instances->{$_[1]};
  my $def = $Registry->{$_[1]} or croak "Database |$_[1]| is not defined";
  return $Instances->{$_[1]} = $_[0]->new
      (sources => $def->{sources} ||
           ($def->{get_sources} or sub { undef })->(),
       onerror => $def->{onerror} ||
           ($def->{get_onerror} or sub { undef })->(),
       onconnect => $def->{onconnect} ||
           ($def->{get_onconnect} or sub { undef })->(),
       schema => $def->{schema} ||
           ($def->{get_schema} or sub { undef })->(),
       master_only => $def->{master_only},
       table_name_normalizer => $def->{table_name_normalizer} ||
           ($def->{get_table_name_normalizer} or sub { undef })->());
} # load

sub create_registry ($;%) {
  shift;
  return bless {
    Registry => {@_},
    Instances => {},
  }, 'Dongry::Database::Registry';
} # new

# ------ Connection ------

sub source ($;$$) {
  my $self = shift;
  my $name = shift || 'default';
  if (@_) {
    $self->{sources}->{$name} = $_[0];
  }
  return $self->{sources}->{$name};
}

sub onconnect ($) {
  if (@_ > 1) {
    $_[0]->{onconnect} = $_[1];
  }
  return $_[0]->{onconnect} || sub {
    #my ($self, %args) = @_;
    #
  };
} # onconnect

sub onerror ($) {
  if (@_ > 1) {
    $_[0]->{onerror} = $_[1];
  }
  return $_[0]->{onerror} || sub {
    my ($self, %args) = @_;
    $args{text} =~ s/\n$//g;
    if ($args{anyevent}) {
      warn $self->source ($args{source_name})->{dsn} .
          ': ' . $args{text} .
          (defined $args{sql} ? ': ' . $args{sql} : '') .
          " at $args{file_name} line $args{line}\n";
      ## Don't die in AnyEvent mode.
    } else {
      local $Carp::CarpLevel = $Carp::CarpLevel - 1;
      croak $self->source ($args{source_name})->{dsn} .
          ': ' . $args{text} .
          (defined $args{sql} ? ': ' . $args{sql} : '');
    }
  };
} # onerror

sub _get_caller () {
  return [caller ((sub { Carp::short_error_loc })->() - 1)];
  # [1] file
  # [2] line
} # _get_caller

sub connect ($$;%) {
  my $self = shift;
  my $name = shift or croak 'No data source name';
  my $source = $self->{sources}->{$name}
      or croak "Data source |$name| is not defined";
  my %args = @_;

  my $return = bless {
    cb => $args{cb},
    caller => _get_caller,
  }, 'Dongry::Database::Executed';

  if ($source->{anyevent}) {
    if ($SQL_DEBUG) {
      eval qq{ require $AESQLDebugClass } or die $@;
    }

    require AnyEvent::MySQL::Client;
    $return->_thenablize;

    ## <http://search.cpan.org/dist/DBD-mysql/lib/DBD/mysql.pm#connect>.
    my %connect;
    my $dsn = $source->{dsn};
    $connect{username} = $source->{username};
    $connect{password} = $source->{password};
    if ($dsn =~ s/^dbi:(?:mysql|MariaDB)://i) {
      my %dsn;
      if ($dsn =~ /[;=]/) {
        for (split /;/, $dsn) {
          my ($n, $v) = split /=/, $_, 2;
          $dsn{$n} = $v;
        }
      } else {
        $dsn{database} = $dsn;
      }
      $connect{username} = delete $dsn{user}
          if not defined $connect{username};
      $connect{password} = delete $dsn{password}
          if not defined $connect{password};
      $connect{database} = defined $dsn{dbname} ? $dsn{dbname} : $dsn{database};
      if (defined $dsn{mysql_socket}) {
        $connect{hostname} = 'unix/';
        $connect{port} = delete $dsn{mysql_socket};
      } else {
        $connect{hostname} = $dsn{host};
        $connect{port} = defined $dsn{port} ? $dsn{port} : 3306;
      }
      if (0 and $source->{tls}) { # future extension...
        $connect{tls} = $source->{tls};
      } else {
        $connect{tls} = {} if delete $dsn{mysql_ssl};
        $connect{tls} = {} if delete $dsn{mariadb_ssl};
        $connect{tls}->{key_file} = delete $dsn{mysql_ssl_client_key}
            if defined $dsn{mysql_ssl_client_key};
        $connect{tls}->{key_file} = delete $dsn{mariadb_ssl_client_key}
            if defined $dsn{mariadb_ssl_client_key};
        $connect{tls}->{cert_file} = delete $dsn{mysql_ssl_client_cert}
            if defined $dsn{mysql_ssl_client_cert};
        $connect{tls}->{cert_file} = delete $dsn{mariadb_ssl_client_cert}
            if defined $dsn{mariadb_ssl_client_cert};
        $connect{tls}->{ca_file} = delete $dsn{mysql_ssl_ca_file}
            if defined $dsn{mysql_ssl_ca_file};
        $connect{tls}->{ca_file} = delete $dsn{mariadb_ssl_ca_file}
            if defined $dsn{mariadb_ssl_ca_file};
        $connect{tls}->{ca_path} = delete $dsn{mysql_ssl_ca_path}
            if defined $dsn{mysql_ssl_ca_path};
        $connect{tls}->{ca_path} = delete $dsn{mariadb_ssl_ca_path}
            if defined $dsn{mariadb_ssl_ca_path};
        $connect{tls}->{cipher_list} = delete $dsn{mysql_ssl_cipher}
            if defined $dsn{mysql_ssl_cipher};
        $connect{tls}->{cipher_list} = delete $dsn{mariadb_ssl_cipher}
            if defined $dsn{mariadb_ssl_cipher};
      }
      delete $dsn{$_} for qw(host port user password dbname database
                             mysql_ssl mysql_ssl_client_key
                             mysql_ssl_client_cert mysql_ssl_ca_file
                             mysql_ssl_ca_path mysql_ssl_cipher
                             mariadb_ssl mariadb_ssl_client_key
                             mariadb_ssl_client_cert mariadb_ssl_ca_file
                             mariadb_ssl_ca_path mariadb_ssl_cipher);
      $connect{error} = "Unknown dsn parameter |@{[join ' ', keys %dsn]}|" if keys %dsn;
    } else {
      $connect{error} = "Non-MySQL database driver is specified: |$dsn|";
    }

    my $onerror_args = {db => $self, caller => $return->{caller}};
    my $connect = sub {
      if (defined $connect{error}) {
        AE::postpone (sub {
          $onerror_args->{db}->{dbhs}->{$name}->disconnect
              if $onerror_args->{db}->{dbhs}->{$name};
          $onerror_args->{db}->{dbhs}->{$name} = bless {
            error_text => $connect{error},
          }, 'Dongry::Database::BrokenConnection';
          $return->_ng ($onerror_args->{db}, bless {
            error_text => $connect{error},
          }, 'Dongry::Database::Executed::NotAvailable');
        });
        return;
      }

      my $timeout = 10;
      my $timer; $timer = AE::timer ($timeout, 0, sub {
        undef $timer;
        $onerror_args->{db}->{dbhs}->{$name}->disconnect
            if $onerror_args->{db}->{dbhs}->{$name};
        $onerror_args->{db}->{dbhs}->{$name} = bless {
          error_text => "$dsn: Connect timeout ($timeout)",
        }, 'Dongry::Database::BrokenConnection';
        $return->_ng ($onerror_args->{db}, bless {
          error_text => "$dsn: Connect timeout ($timeout)",
        }, 'Dongry::Database::Executed::NotAvailable');
      });

      $self->{dbhs}->{$name}->disconnect if $self->{dbhs}->{$name};
      $self->{dbhs}->{$name} = AnyEvent::MySQL::Client->new;
      $self->{dbhs}->{$name}->connect (%connect)->then (sub {
        undef $timer;
        if ($onerror_args->{db}) {
          eval {
            local $onerror_args->{db}->{reconnect_disabled}->{$name} = 1;
            local $onerror_args->{db}->{onconnect_running}->{$name} = 1;
            $onerror_args->{db}->onconnect->($onerror_args->{db}, source_name => $name);
          };
          warn "Died within handler: $@" if $@;
          $return->_ok ($onerror_args->{db}, bless {}, 'Dongry::Database::Executed::NoResult');
        }
      }, sub {
        undef $timer;
        my $error_text = ''.$_[0];
        $onerror_args->{db}->{dbhs}->{$name} = bless {
          error_text => "$dsn: $error_text",
        }, 'Dongry::Database::BrokenConnection';
        my $file_name = $onerror_args->{caller}->[1];
        my $line = $onerror_args->{caller}->[2];
        eval {
          $onerror_args->{db}->onerror->($onerror_args->{db},
                                         anyevent => 1,
                                         text => "$dsn: $error_text",
                                         file_name => $file_name,
                                         line => $line,
                                         source_name => $name,
                                         sql => $args{_sql});
        };
        warn "Died within handler: $@" if $@;
        $return->_ng ($onerror_args->{db}, bless {
          error_text => "$dsn: $error_text",
        }, 'Dongry::Database::Executed::NotAvailable');
      })->catch (sub {
        warn "Died within handler: $_[0]";
      })->then (sub { undef $onerror_args; undef $return });
    }; # $connect

    if ($self->{reconnect_disabled}->{$name}) {
      $return->_ok ($self, bless {}, 'Dongry::Database::Executed::NoResult');
    } elsif (my $client = $self->{dbhs}->{$name}) {
      $client->ping->then (sub {
        if ($_[0]) {
          $return->_ok ($self, bless {}, 'Dongry::Database::Executed::NoResult');
        } else {
          $connect->();
        }
      })->catch (sub {
        warn "Died within handler: $_[0]";
      });
    } else {
      $connect->();
    }
    return $return;
  } else { # DBI
    if ($SQL_DEBUG) {
      eval qq{ require $SQLDebugClass } or die $@;
    }

    if ($self->{dbhs}->{$name}) {
      if ($self->{dbhs}->{$name}->ping) {
        $return->_ok ($self, bless {}, 'Dongry::Database::Executed::NoResult');
        return $return;
      } else {
        $self->disconnect ($name);
      }
    }

    require DBI;
    my $onerror_args = {db => $self};
    weaken $onerror_args->{db};
    $self->{dbhs}->{$name} = DBI->connect
        ($source->{dsn}, $source->{username}, $source->{password},
         {RaiseError => 1, PrintError => 0, HandleError => sub {
            #my ($msg, $dbh, $returned) = @_:
            {
              local $Carp::CarpLevel = $Carp::CarpLevel + 1; 
              $onerror_args->{db}->onerror
                  ->($onerror_args->{db},
                     text => $_[0],
                     source_name => $name,
                     sql => $onerror_args->{db}->{last_sql});
            }
            croak $_[0];
            #return 0;
          }, AutoCommit => 1, ReadOnly => !$source->{writable},
          AutoInactiveDestroy => 1});
    {
      local $self->{onconnect_running}->{$name} = 1;
      $self->onconnect->($self, source_name => $name);
    }
    $return->_ok ($self, bless {}, 'Dongry::Database::Executed::NoResult');
    return $return;
  }
} # connect

sub disconnect ($;$%) {
  my ($self, $_name, %args) = @_;
  my $return = bless {
    cb => $args{cb},
    caller => _get_caller,
  }, 'Dongry::Database::Executed';

  my @then;
  my $has_promise;
  for my $name (
    defined $_name ? ($_name) : (keys %{$self->{sources} or {}})
  ) {
    if ($self->{in_transaction} and $name eq 'master') {
      carp "$self->{sources}->{$name}->{dsn}: A transaction is rollbacked because the database is disconnected before the transaction is committed";
      if ($self->{sources}->{$name}->{anyevent}) {
        $self->{dbhs}->{$name}->rollback (sub { });
      } else {
        $self->{dbhs}->{$name}->rollback;
      }
      delete $self->{in_transaction};
    }

    if (defined $self->{execute_promise}->{$name}) {
      push @then, $self->{execute_promise}->{$name} = $self->{execute_promise}->{$name}->then (sub {
        my $client = delete $self->{dbhs}->{$name};
        return $client->disconnect if defined $client;
      })->then (sub {
        delete $self->{execute_promise}->{$name};
        return undef;
      });
    } elsif ($self->{dbhs}->{$name}) {
      my $result = $self->{dbhs}->{$name}->disconnect;
      if (UNIVERSAL::can ($result, 'then') and $result->can ('then')) {
        push @then, $result;
      }
      delete $self->{dbhs}->{$name};
    }
    if ($self->{sources}->{$name}->{anyevent}) {
      $has_promise = 1;
    }
  } # $name
  $return->_thenablize if $has_promise;
  if (@then) {
    (ref $then[0])->all (\@then)->then (sub {
      $return->_ok ($self);
    }, sub {
      $return->_ok ($self);
    });
  } else {
    $return->_ok ($self);
  }
  return $return;
} # disconnect

sub DESTROY {
  my $self = $_[0];
  my @p = grep { defined $_ } values %{$self->{execute_promise} or {}};
  if (@p) {
    AnyEvent::MySQL::Client::Promise->all (\@p)->then (sub { $self->disconnect });
  } else {
    $self->disconnect;
  }

  local $@;
  eval { die };
  warn "$$: Reference to " . $_[0]->debug_info . " is not discarded before global destruction\n"
      if $@ =~ /during global destruction/;
} # DESTROY

# ------ Transaction and source selection ------

sub transaction ($;%) {
  my ($self, %args) = @_;
  croak "Can't start new transaction before committing the current transaction"
      if $self->{in_transaction};
  croak "Can't start new transaction while a source is forced"
      if $self->{force_source_name};

  my $source_name = 'master';
  if ($self->{sources}->{$source_name}->{anyevent}) {
    return $self->_ae_transaction_start ($source_name);
  } else {
    $self->connect ($source_name);
    $self->{in_transaction} = 1;
    $self->{dbhs}->{$source_name}->begin_work ($args{cb});
    return bless {db => $self}, 'Dongry::Database::Transaction';
  }
} # transaction

sub force_source_name ($$) {
  my $self = shift;
  croak "Can't force source in a transaction" if $self->{in_transaction};
  croak "Can't force source while another source is forced"
      if $self->{force_source_name};

  if ($self->{sources}->{$_[0]} and $self->{sources}->{$_[0]}->{anyevent}) {
    # croak if $self->{ae_transaction_promise}->{...}
    croak "|force_source_name| is not supported in asynchronous mode";
  }

  $self->{force_source_name} = $_[0];
  return bless {db => $self, source_name => $_[0]},
      'Dongry::Database::ForceSource';
} # force_source_name

# ------ Bare SQL operations ------

our $ReadOnlyQueryPattern = qr/^\s*(?:
  [Ss][Ee][Ll][Ee][Cc][Tt]|
  [Ss][Hh][Oo][Ww]|
  [Dd][Ee][Ss][Cc](?:[Rr][Ii][Bb][Ee])?|
  [Ee][Xx][Pp][Ll][Aa][Ii][Nn]
)\b/x;

our $EmbedCallerInSQL;

if ($ENV{SQL_DEBUG} && $ENV{SQL_DEBUG} =~ /embed_caller/) {
  $EmbedCallerInSQL ||= 1;
}

our $RetryIfDeadlock;
our $MaxRetryCount ||= 2;

sub execute ($$;$%) {
  my ($self, $sql, $values, %args) = @_;

  my $name = $args{source_name} ||
      ($self->{force_source_name}
         ? $self->{force_source_name}
         : $self->{master_only}
           ? 'master'
           : ((!$self->{in_transaction} &&
               !$args{must_be_writable} &&
               $sql =~ /$ReadOnlyQueryPattern/o)
                  ? 'default' : 'master'));
  if ($name ne 'master' and $self->{in_transaction}) {
    croak "Data source |$name| cannot be used in transaction";
  }
  if ($self->{force_source_name} and 
      $self->{force_source_name} ne $name) {
    croak sprintf
        "Data source |%s| cannot be used while the source is forced to |%s|",
        $name, $self->{force_source_name};
  }
  croak "Data source |$name| is not defined"
      unless $self->{sources}->{$name};

  if (not $self->{sources}->{$name}->{writable} and
      ($args{must_be_writable} or
       (not $args{even_if_read_only} and
        not $sql =~ /$ReadOnlyQueryPattern/o))) {
    croak "Data source |$name| is read-only";
  }

  if (defined $values and ref $values eq 'HASH') {
    ($sql, $values) = _where [$sql, %$values];
  }
  
  if (defined $self->{sources}->{$name}->{sql_comment}) {
    my $comment = $self->{sources}->{$name}->{sql_comment};
    $comment =~ s{\*/}{\* /}g;
    $sql .= ' /* ' . $comment . ' */';
  }
  my $caller;
  if ($EmbedCallerInSQL) {
    $caller //= _get_caller;
    my $text = $name . ' at ' . $caller->[1] . ' line ' . $caller->[2];
    $text =~ s{\*/}{\* /}g;
    $sql .= ' /* ' . $text . ' */';
  }

  if ($self->{sources}->{$name}->{anyevent}) {
    $caller //= _get_caller;
    my $onerror_args = {caller => $caller};
    my $return = bless {
      cb => $args{cb},
      caller => $onerror_args->{caller},
    }, 'Dongry::Database::Executed::NoResult';
    $return->_thenablize;

    # XXX retry

    if ($args{each_as_row_cb}) {
      require Dongry::Table;
      croak 'Table name is not known' if not defined $args{table_name};
    }

    my $p;
    if ($args{_ae_transaction}) {
      if ($self->{onconnect_running}->{$name}) {
        return Promise->reject (bless {
          error_text => "Transaction is not allowed within onconnect handler",
          error_sql => $sql,
        }, 'Dongry::Database::Executed::NotAvailable');
      }
      $p = $self->{ae_transaction_promise}->{$name}->[0];
    } else {
      if (defined $self->{execute_promise}->{$name} and
          not $self->{onconnect_running}->{$name}) {
        $p = $self->{execute_promise}->{$name}->then (sub {
          return $self->connect ($name, _sql => $sql);
        });
      } else {
        $p = $self->connect ($name, _sql => $sql);
      }
      $p = $p->then (undef, sub {
        my $error = $_[0];
        die bless {
          error_text => "|connect| failed ($error)",
          error_sql => $sql,
          _skip_onerror => 1,
        }, 'Dongry::Database::Executed::NotAvailable';
      });
    }

    my $client;
    $p = $p->then (sub {
      $client = $self->{dbhs}->{$name} || bless {
        error_text => 'Connection is lost during event loop',
      }, 'Dongry::Database::BrokenConnection';
      return $client->statement_prepare ($self->{last_sql} = $sql);
    })->then (sub {
      my $x = $_[0];
      die $x unless $x->is_success;
      my @row;
      return $client->statement_execute ($x->packet->{statement_id}, [map { {type => 'BLOB', value => $_}; } @{$values or []}], sub {
        my $cols = $_[0]->column_packets;
        my $data = $_[0]->packet->{data};
        my $row = {map { $cols->[$_]->{name} => $data->[$_]->{value} } 0..$#$data};
        if ($args{each_as_row_cb}) {
          local $_ = bless {
            db => $self,
            table_name => $args{table_name},
            data => $row,
          }, 'Dongry::Table::Row';
          $args{each_as_row_cb}->();
        } elsif ($args{each_cb}) {
          local $_ = $row;
          $args{each_cb}->();
        } else {
          push @row, $row;
        }
      })->then (sub { # execute
        my $y = $_[0];
        die bless {
          error_text => ''.$y, error_sql => $sql,
        }, 'Dongry::Database::Executed::NotAvailable' unless $y->is_success;

        my $result = bless {
          db => $self,
          table_name => $args{table_name},
        }, 'Dongry::Database::Executed::Inserted';
        if ($y->packet->{header} == 0) { # OK_Packet
          $result->{row_count} = $y->packet->{affected_rows};
        } else {
          $result->{row_count} = @row;
          unless ($args{each_as_row_cb} or $args{each_cb}) {
            $result->{data} = \@row;
          }
        }
        $result->{no_each} = 1;
        $return->_ok ($self, $result); # or throw
      })->then (sub {
        return $client->statement_close ($x->packet->{statement_id});
      }, sub {
        my $error = $_[0];
        return $client->statement_close ($x->packet->{statement_id})->then (sub { die $error });
      });
    })->catch (sub {
      my $x = $_[0];

      my $result;
      if (UNIVERSAL::isa ($x, 'Dongry::Database::Executed')) {
        $result = $x;
      } else {
        $result = bless {error_text => ''.$x, error_sql => $sql},
            'Dongry::Database::Executed::NotAvailable';
      }
      
      my $file_name = $onerror_args->{caller}->[1];
      my $line = $onerror_args->{caller}->[2];
      eval {
        $return->_ng ($self, $result);
      };
      warn "Died within handler: $@" if $@;
      return if $result->{_skip_onerror};
      $self->onerror->($self,
                       anyevent => 1,
                       text => $result->{error_text},
                       file_name => $file_name,
                       line => $line,
                       source_name => $name,
                       sql => $sql); # or throw
    })->catch (sub {
      warn "Died within handler: $_[0]";
    })->then (sub {
      undef $client; undef $self; %args = (); undef $return;
    }); # $p

    if ($args{_ae_transaction}) {
      $self->{ae_transaction_promise}->{$name}->[0] = $p;
    } else {
      $self->{execute_promise}->{$name} = $p;
    }
    
    return $return;
  } else {
    $self->connect ($name, _sql => $sql);
    my $dbh = $self->{dbhs}->{$name};
    my $sth;
    my $rows;
    my $retry = 0;
    {
      my $redo = 0;
      local $dbh->{RaiseError} = 0;
      my $orig_onerror = $dbh->{HandleError};
      local $dbh->{HandleError} = sub {
        my $error_type = $_[0];
        if ($RetryIfDeadlock and
            $retry <= $MaxRetryCount and
            $error_type =~ /^DBD::mysql::st execute failed: (?:Deadlock found when trying to get lock|Lock wait timeout exceeded); try restarting transaction/) {
          $redo = 1;
          return;
        }
        $_[0] = "[retry=$retry] $_[0]" if $retry;
        $orig_onerror->(@_);
        die $_[0];
      };
      $sth = $dbh->prepare ($self->{last_sql} = $sql);
      $rows = $sth->execute (@{$values or []});
      if ($redo) {
        $retry++;
        redo;
      }
    };

    my $result = bless {db => $self, sth => $sth, row_count => $rows,
                        table_name => $args{table_name}, cb => $args{cb}},
        'Dongry::Database::Executed';
    if ($args{each_as_row_cb}) {
      $result->each_as_row ($args{each_as_row_cb});
    } elsif ($args{each_cb}) {
      $result->each ($args{each_cb});
    }
    $result->_ok ($self, $result);
    return $result;
  }
} # execute

sub _ae_transaction_start ($$) {
  my ($self, $source_name) = @_;

  if ($self->{onconnect_running}->{$source_name}) {
    return Promise->reject (bless {
      error_text => "Transaction is not allowed within onconnect handler",
      error_sql => 'begin',
    }, 'Dongry::Database::Executed::NotAvailable');
  }
  
  my $p;
  if (defined $self->{execute_promise}->{$source_name}) {
    $p = $self->{execute_promise}->{$source_name}->then (sub {
      return $self->connect ($source_name);
    });
  } else {
    $p = $self->connect ($source_name);
  }

  my ($r, $s);
  $r = Promise->new (sub { ($s) = @_ });
  $self->{execute_promise}->{$source_name} = $r;
  
  return $p->then (sub {
    my $client = $self->{dbhs}->{$source_name} || bless {
      error_text => 'Connection is lost during event loop',
    }, 'Dongry::Database::BrokenConnection';
    die "ae_transaction_promise $source_name is busy" # assert
        if defined $self->{ae_transaction_promise}->{$source_name};
    my $q = $client->query ('begin')->then (sub {
      return bless {db => $self}, 'Dongry::Database::AETransaction';
    }, sub {
      my $x = $_[0];
      my $result;
      if (UNIVERSAL::isa ($x, 'Dongry::Database::Executed')) {
        $result = $x;
      } else {
        $result = bless {error_text => ''.$x, error_sql => 'begin'},
            'Dongry::Database::Executed::NotAvailable';
      }
      die $result;
    });

    $self->{ae_transaction_promise}->{$source_name} = [$q, $s];
    return $q;
  });
} # _ae_transaction_start

sub _ae_transaction_end ($$$) {
  my ($self, $source_name, $query) = @_;
  my $v = $self->{ae_transaction_promise}->{$source_name};
  return $v->[0]->then (sub {
    my $client = $self->{dbhs}->{$source_name} || bless {
      error_text => 'Connection is lost during event loop',
    }, 'Dongry::Database::BrokenConnection';
    return $client->query ($query)->then (sub {
      delete $self->{ae_transaction_promise}->{$source_name};
      my $result = bless {}, 'Dongry::Database::Executed::NoResult';
      $v->[1]->();
      return $result;
    }, sub {
      my $x = $_[0];
      delete $self->{ae_transaction_promise}->{$source_name};
      my $result;
      if (UNIVERSAL::isa ($x, 'Dongry::Database::Executed')) {
        $result = $x;
      } else {
        $result = bless {error_text => ''.$x, error_sql => $query},
            'Dongry::Database::Executed::NotAvailable';
      }
      delete $self->{dbhs}->{$source_name};
      return $client->disconnect->then (sub {
        $v->[1]->();
        die $result;
      }, sub {
        $v->[1]->();
        die $result;
      });
    });
  });
} # _ae_transaction_end

# ------ Structured SQL executions ------

# XXX promise not supported
sub set_tz ($;$%) {
  my ($self, $tz, %args) = @_;
  $tz ||= '+00:00';
  $self->execute ('SET time_zone = ?', [$tz],
                  source_name => $args{source_name},
                  even_if_read_only => 1,
                  cb => $args{cb},
                  _ae_transaction => $args{_ae_transaction});
  return undef;
} # set_tz

# XXX cb/promise not supported
sub has_table ($$;%) {
  my ($self, $name, %args) = @_;
  my $row = $self->execute('SHOW TABLES LIKE :table', {
    table => Dongry::SQL::like ($name),
  }, source_name => $args{source_name}, _ae_transaction => $args{_ae_transaction})->first;
  return $row && [values %$row]->[0] eq $name;
} # has_table

sub insert ($$$;%) {
  my ($self, $table_name, $data, %args) = @_;

  croak "No data" unless @$data;
  my %col;
  for (@$data) {
    $col{$_} = 1 for keys %$_;
  }

  my @col = sort { $a cmp $b } keys %col;
  my @values;
  my @placeholders;
  for my $data (@$data) {
    push @values, (map {
      exists $data->{$_}
          ? (defined $data->{$_} and
             ref $data->{$_} eq 'Dongry::SQL::BareFragment')
              ? ()
              : ($data->{$_})
          : ()
    } @col);
    push @placeholders, '(' . (join ', ', (map {
      exists $data->{$_}
          ? (defined $data->{$_} and
             ref $data->{$_} eq 'Dongry::SQL::BareFragment')
              ? ${$data->{$_}}
              : '?'
          : 'DEFAULT'
    } @col)) . ')';
  } # $data
  croak "Too many values" if @values > 20000;
  
  my $sql = 'INSERT';
  if ($args{duplicate}) {
    $sql .= ' IGNORE' if $args{duplicate} eq 'ignore';
    $sql = 'REPLACE' if $args{duplicate} eq 'replace';
  }
  $sql .= ' INTO ' . (_quote $table_name) .
      ' (' . (join ', ', map { _quote $_ } @col) . ')' .
      ' VALUES ' . (join ', ', @placeholders);
  if ($args{duplicate} and ref $args{duplicate} eq 'HASH') {
    my $value = $args{duplicate};
    my @col = sort { $a cmp $b } keys %$value;
    croak 'Duplicate hash is empty' unless @col;
    my @sql_value;
    for (@col) {
      if (defined $value->{$_} and
          ref $value->{$_} eq 'Dongry::SQL::BareFragment') {
        push @sql_value, (_quote $_), ${$value->{$_}};
      } else {
        push @sql_value, (_quote $_), '?';
        push @values, $value->{$_};
      }
    }
    $sql .= sprintf ' ON DUPLICATE KEY UPDATE '
         . (join ', ', ('%s = %s') x (@sql_value / 2)),
        @sql_value;
  } elsif ($args{duplicate} and ref $args{duplicate} eq 'ARRAY') {
    my $value = $args{duplicate};
    my @col = @$value;
    croak 'Duplicate array is empty' unless @col;
    my @sql_value;
    while (@col) {
      my $name = shift @col;
      my $value = shift @col;
      if (defined $value and ref $value eq 'Dongry::SQL::BareFragment') {
        push @sql_value, (_quote $name), ${$value};
      } else {
        push @sql_value, (_quote $name), '?';
        push @values, $value;
      }
    }
    $sql .= sprintf ' ON DUPLICATE KEY UPDATE '
         . (join ', ', ('%s = %s') x (@sql_value / 2)),
        @sql_value;
  } # duplicate

  my $cb_orig = $args{cb} || sub { };
  my $cb = sub {
    unless ($_[1]->is_error) {
      bless $_[1], 'Dongry::Database::Executed::Inserted';
      $_[1]->{table_name} = $table_name;
      $_[1]->{data} = $data;
    }
    goto &$cb_orig;
  }; # $cb

  my $return = $self->execute
      ($sql, \@values, source_name => $args{source_name}, cb => $cb,
       _ae_transaction => $args{_ae_transaction});

  return unless defined wantarray;
  return $return if $return->is_error or $return->can ('then');
  bless $return, 'Dongry::Database::Executed::Inserted';
  $return->{table_name} = $table_name;
  $return->{data} = $data;
  return $return;
} # insert

sub select ($$$;%) {
  my ($self, $table_name, $where, %args) = @_;

  if ($args{and_where}) {
    $where = [':w1:sub AND :w2:sub', w1 => $where, w2 => $args{and_where}];
  }
  my ($where_sql, $where_bind) = _where ($where, $args{_table_schema});
  croak 'No where' unless $where_sql;
  
  my $sql = 'SELECT';
  $sql .= ' DISTINCT' if $args{distinct};
  if ($args{fields}) {
    $sql .= ' ' . _fields $args{fields};
  } else {
    $sql .= ' *';
  }
  $sql .= ' FROM ' . (_quote $table_name)
       . ' WHERE ' . $where_sql;
  if ($args{group}) {
    $sql .= ' GROUP BY ' . join ', ', map { _quote $_ } @{$args{group}};
  }
  $sql .= ' ORDER BY ' . _order ($args{order}) if $args{order};
  $sql .= sprintf ' LIMIT %d,%d', ($args{offset} || 0), ($args{limit} || 1)
      if defined $args{limit} or defined $args{offset};
  if ($args{lock}) {
    carp "Lock used outside of transaction"
        unless $self->{in_transaction} or $args{_ae_transaction};
    $sql .= ' FOR UPDATE' if $args{lock} eq 'update';
    $sql .= ' LOCK IN SHARE MODE' if $args{lock} eq 'share';
    $args{must_be_writable} = 1;
  }

  my $return = $self->execute
      ($sql, $where_bind,
       source_name => $args{source_name},
       must_be_writable => $args{must_be_writable},
       each_cb => $args{each_cb},
       table_name => $table_name,
       each_as_row_cb => $args{each_as_row_cb},
       cb => $args{cb},
       _ae_transaction => $args{_ae_transaction});
  return unless defined wantarray;

  $return->{table_name} = $table_name
      if not $return->is_error and not $return->can ('then');
  return $return;
} # select

sub update ($$$%) {
  my ($self, $table_name, $value, %args) = @_;
  
  my @col = sort { $a cmp $b } keys %$value;
  croak 'No value to update' unless @col;

  my ($where_sql, $where_bind) = _where ($args{where}, $args{_table_schema});
  croak 'No where' unless $where_sql;

  my $sql .= 'UPDATE';
  $sql .= ' IGNORE' if $args{duplicate} and $args{duplicate} eq 'ignore';
  $sql .= ' ' . _quote $table_name;

  my @sql_value;
  my @bound_value;
  for (@col) {
    if (defined $value->{$_} and
        ref $value->{$_} eq 'Dongry::SQL::BareFragment') {
      push @sql_value, (_quote $_), ${$value->{$_}};
    } else {
      push @sql_value, (_quote $_), '?';
      push @bound_value, $value->{$_};
    }
  }
  $sql .= sprintf ' SET ' . (join ', ', ('%s = %s') x (@sql_value / 2)),
      @sql_value;

  $sql .= ' WHERE ' . $where_sql;
  $sql .= ' ORDER BY ' . _order ($args{order}) if $args{order};
  croak 'Offset is not supported' if defined $args{offset};
  $sql .= sprintf ' LIMIT %d', $args{limit} || 1 if defined $args{limit};

  return $self->execute
     ($sql, [@bound_value, @$where_bind], source_name => $args{source_name},
      cb => $args{cb},
      _ae_transaction => $args{_ae_transaction});
} # update

sub delete ($$$;%) {
  my ($self, $table_name, $where, %args) = @_;

  my ($where_sql, $where_bind) = _where ($where, $args{_table_schema});
  croak 'No where' unless $where_sql;
  
  my $sql = 'DELETE FROM ' . (_quote $table_name) . ' WHERE ' . $where_sql;
  $sql .= ' ORDER BY ' . _order ($args{order}) if $args{order};
  croak 'Offset is not supported' if defined $args{offset};
  $sql .= sprintf ' LIMIT %d', $args{limit} || 1 if defined $args{limit};

  return $self->execute
      ($sql, $where_bind, source_name => $args{source_name},
       cb => $args{cb},
       _ae_transaction => $args{_ae_transaction});
} # delete

sub uuid_short ($$;%) {
  my ($self, $n, %args) = @_;
  croak "Bad ID count $n" if $n < 1;
  my $sql = 'SELECT ' . join ', ', map { 'UUID_SHORT() AS `'.$_.'`' } 1..$n;
  return $self->execute ($sql, {}, source_name => $args{source_name} // 'master')->then (sub {
    return [values %{$_[0]->all->[0]}];
  });
} # uuid_short

sub bare_sql_fragment ($$) {
  return bless \('' . $_[1]), 'Dongry::SQL::BareFragment';
} # bare_sql_fragment

# ------ Schema-aware operations ------

use Dongry::Type;

sub schema ($) {
  if (@_ > 1) {
    $_[0]->{schema} = $_[1];
  }
  return $_[0]->{schema};
} # schema

sub table_name_normalizer ($) {
  if (@_ > 1) {
    $_[0]->{table_name_normalizer} = $_[1];
  }
  return $_[0]->{table_name_normalizer} || sub { $_[0] };
} # table_name_normalizer

sub table ($$) {
  croak 'No table name' unless defined $_[1];
  require Dongry::Table;
  return Dongry::Table->new
      (db => $_[0], table_name => $_[1]);
} # table

sub query ($%) {
  my ($self, %args) = @_;
  my $query_class = $args{query_class} || 'Dongry::Query';
  eval qq{ require $query_class } or die $@;
  return $query_class->new (db => $self, %args);
} # query

# ------ Debug information ------

sub debug_info ($) {
  my $self = shift;
  return sprintf '{DB: %s}',
      join '; ',
          map { $_ . ' = ' . ($self->{sources}->{$_}->{label} ||
                              $self->{sources}->{$_}->{dsn}) }
          keys %{$self->{sources} or {}};
} # debug_info

# ------ Operation results ------

package Dongry::Database::Executed;
our $VERSION = '2.0';
use Carp;
use overload bool => sub { 1 }, '""' => sub {
  my $text = $_[0]->debug_info;
  if (defined $_[0]->{caller}) {
    return sprintf "%s at %s line %s.\n",
      $text, $_[0]->{caller}->[1], $_[0]->{caller}->[2];
  } else {
    return $text;
  }
}, fallback => 1;

push our @CARP_NOT, qw(Dongry::Database);

sub is_success ($) { 1 }
sub is_error ($) { 0 }

sub error_text ($) {
  return $_[0]->{error_text};
} # error_text

sub error_sql ($) {
  return $_[0]->{error_sql};
} # error_sql

sub row_count ($) {
  return $_[0]->{row_count} + 0;
} # row_count

sub table_name ($) {
  if (@_ > 1) {
    $_[0]->{table_name} = $_[1];
  }
  return $_[0]->{table_name};
} # table_name

sub each ($$) {
  my ($self, $code) = @_;
  my $sth = delete $self->{sth} or croak 'This method is no longer available';
  while (my $hashref = $sth->fetchrow_hashref) {
    local $_ = $hashref; ## Sigh, consistency with List::Rubyish...
    $code->();
  }
  $sth->finish;
} # each

sub each_as_row ($$) {
  my ($self, $code) = @_;
  my $tn = $self->{table_name};
  croak 'Table name is not known' if not defined $tn;
  my $db = $self->{db};
  require Dongry::Table;
  $self->each (sub {
    local $_ = bless {db => $db, table_name => $tn, data => $_},
        'Dongry::Table::Row';
    $code->();
  });
} # each_as_row

sub all ($) {
  my $sth = delete $_[0]->{sth} or croak 'This method is no longer available';
  my $list;
  my $err;
  {
    local $@;
    eval {
      $list = $_[0]->{db}->_list ($sth->fetchall_arrayref ({}));
      $sth->finish;
      1;
    } or do {
      $err = $@;
    };
  }
  if ($err) {
    warn $err;
    croak 'This method is not available';
  }
  return $list;
} # all

sub all_as_rows ($) {
  my $tn = $_[0]->{table_name};
  croak 'Table name is not known' if not defined $tn;
  my $db = $_[0]->{db};
  require Dongry::Table;
  return scalar $_[0]->all->map(sub {
    return bless {db => $db, table_name => $tn, data => $_},
        'Dongry::Table::Row';
  });
} # all_as_rows

sub first ($) {
  my $sth = delete $_[0]->{sth} or croak 'This method is no longer available';
  my $first = $sth->fetchrow_hashref; # or undef
  $sth->finish;
  return $first;
} # first

sub first_as_row ($) {
  my $self = shift;
  croak 'Table name is not known' if not defined $self->{table_name};
  my $data = $self->first or return undef;
  require Dongry::Table;
  return bless {db => $self->{db},
                table_name => $self->{table_name},
                data => $data}, 'Dongry::Table::Row';
} # first_as_row

sub _ok ($$$) {
  my $self = $_[0];
  $self->{promise_ok}->($_[2]) if $self->{promise_ok};
  delete $self->{promise_ok};
  delete $self->{promise_ng};
  (delete $self->{cb})->($_[1], $_[2]) if $self->{cb};
  $_[2]->{caller} ||= $self->{caller} if $_[2];
  return;
} # _ok

sub _ng ($$$) {
  my $self = $_[0];
  $self->{promise_ng}->($_[2]) if $self->{promise_ng};
  delete $self->{promise_ok};
  delete $self->{promise_ng};
  (delete $self->{cb})->($_[1], $_[2]) if $self->{cb};
  $_[2]->{caller} ||= $self->{caller} if $_[2];
  return;
} # _ng

sub _thenablize ($) {
  my $self = $_[0];
  require AnyEvent::MySQL::Client::Promise;
  $self->{promise} = AnyEvent::MySQL::Client::Promise->new (sub {
    $self->{promise_ok} = $_[0];
    $self->{promise_ng} = $_[1];
  });
} # _thenablize

sub then ($;$$) {
  my $self = $_[0];
  croak 'This object is not thenable' unless $self->{promise};
  return $self->{promise}->then ($_[1], $_[2]);
} # then

sub can {
  if ($_[1] eq 'then') {
    return 0 unless ref $_[0] and $_[0]->{promise};
  }
  return shift->SUPER::can (@_);
} # can

sub debug_info ($) {
  my $self = shift;
  my @info;
  push @info, 'error' if $self->is_error;
  for my $name (qw(table_name error_text error_sql)) {
    my $v = $self->$name;
    push @info, $name . ' = ' . $v if defined $v;
  }
  push @info, 'file = ' . $_[0]->{caller}->[1] if defined $_[0]->{caller}->[1];
  push @info, 'line = ' . $_[0]->{caller}->[2] if defined $_[0]->{caller}->[2];
  return sprintf '{DBExecuted: %s}', join '; ', @info;
} # debug_info

sub DESTROY {
  $_[0]->{sth}->finish if $_[0]->{sth};

  local $@;
  eval { die };
  warn "$$: Reference to " . $_[0]->debug_info . " is not discarded before global destruction\n"
      if $@ =~ /during global destruction/;
} # DESTROY

package Dongry::Database::Executed::Inserted;
our $VERSION = '1.0';
push our @ISA, 'Dongry::Database::Executed';
use Carp;

sub each ($$) {
  my ($self, $code) = @_;
  my $data = delete $self->{data}
      or croak 'This method is no longer available';
  croak 'This method is not available' if $self->{no_each};
  $code->() for @$data;
  delete $self->{data};
} # each

sub each_as_row ($$) {
  my ($self, $code) = @_;
  my $tn = $self->{table_name};
  croak 'Table name is not known' if not defined $tn;
  my $data = delete $self->{data}
      or croak 'This method is no longer available';
  croak 'This method is not available' if $self->{no_each};
  my $db = $self->{db};
  require Dongry::Table;
  for (0..$#$data) {
    local $_ = bless {db => $db, table_name => $tn, data => $data->[$_],
                      $self->{parsed_data}
                          ? (parsed_data => $self->{parsed_data}->[$_]) : ()},
                     'Dongry::Table::Row';
    $code->();
  }
} # each_as_row

sub all ($) {
  my $data = delete $_[0]->{data}
      or croak 'This method is no longer available';
  delete $_[0]->{data};
  return ref $data eq 'ARRAY' ? $_[0]->{db}->_list ($data) : $data;
} # all

sub all_as_rows ($) {
  my $self = shift;
  my $tn = $self->{table_name};
  croak 'Table name is not known' if not defined $tn;
  my $data = delete $self->{data}
      or croak 'This method is no longer available';
  delete $self->{data};
  my $db = $self->{db};
  require Dongry::Table;
  return $db->_list([map {
    bless {db => $db, table_name => $tn, data => $data->[$_],
           $self->{parsed_data}
               ? (parsed_data => $self->{parsed_data}->[$_]) : ()},
          'Dongry::Table::Row';
  } 0..$#$data]);
} # all_as_rows

sub first ($) {
  my $data = delete $_[0]->{data}
      or croak 'This method is no longer available';
  delete $_[0]->{data};
  return $data->[0]; # or undef
} # first

sub first_as_row ($) {
  my $self = shift;
  croak 'Table name is not known' if not defined $self->{table_name};
  my $data = delete $self->{data}
      or croak 'This method is no longer available';
  delete $self->{data};
  return undef unless $data->[0];

  require Dongry::Table;
  return bless 
      {db => $self->{db},
       table_name => $self->{table_name},
       data => $data->[0],
       ($self->{parsed_data}
           ? (parsed_data => $self->{parsed_data}->[0]) : ())},
      'Dongry::Table::Row';
} # first_as_row

package Dongry::Database::Executed::NotAvailable;
our $VERSION = '1.0';
push our @ISA, 'Dongry::Database::Executed';
use Carp;

sub is_success ($) { 0 }
sub is_error ($) { 1 }

sub row_count ($) {
  croak "Result is not available";
} # row_count

package Dongry::Database::Executed::NoResult;
our $VERSION = '1.0';
push our @ISA, 'Dongry::Database::Executed';
use Carp;

sub is_success ($) { 1 }
sub is_error ($) { 0 }

sub row_count ($) {
  croak "Result is not available";
} # row_count

# ------ Transaction ------

package Dongry::Database::AETransaction;
our $VERSION = '1.0';

sub commit ($) {
  return Promise->reject (bless {
    error_text => 'Transaction object is invalid', error_sql => 'commit',
  }, 'Dongry::Database::Executed::NotAvailable') unless defined $_[0]->{db};
  my $db = delete $_[0]->{db};
  return $db->_ae_transaction_end ('master', 'commit');
} # commit

sub rollback ($) {
  return Promise->reject (bless {
    error_text => 'Transaction object is invalid', error_sql => 'rollback',
  }, 'Dongry::Database::Executed::NotAvailable') unless defined $_[0]->{db};
  my $db = delete $_[0]->{db};
  return $db->_ae_transaction_end ('master', 'rollback');
} # rollback

sub execute ($@) {
  return Promise->reject (bless {
    error_text => 'Transaction object is invalid',
  }, 'Dongry::Database::Executed::NotAvailable') unless defined $_[0]->{db};
  return shift->{db}->execute (@_, _ae_transaction => 1, source_name => 'master');
} # execute

sub insert ($@) {
  return Promise->reject (bless {
    error_text => 'Transaction object is invalid',
  }, 'Dongry::Database::Executed::NotAvailable') unless defined $_[0]->{db};
  return shift->{db}->insert (@_, _ae_transaction => 1, source_name => 'master');
} # insert

sub select ($@) {
  return Promise->reject (bless {
    error_text => 'Transaction object is invalid',
  }, 'Dongry::Database::Executed::NotAvailable') unless defined $_[0]->{db};
  return shift->{db}->select (@_, _ae_transaction => 1, source_name => 'master');
} # select

sub update ($@) {
  return Promise->reject (bless {
    error_text => 'Transaction object is invalid',
  }, 'Dongry::Database::Executed::NotAvailable') unless defined $_[0]->{db};
  return shift->{db}->update (@_, _ae_transaction => 1, source_name => 'master');
} # update

sub delete ($@) {
  return Promise->reject (bless {
    error_text => 'Transaction object is invalid',
  }, 'Dongry::Database::Executed::NotAvailable') unless defined $_[0]->{db};
  return shift->{db}->delete (@_, _ae_transaction => 1, source_name => 'master');
} # delete

sub debug_info ($) {
  if (not defined $_[0]->{db}) {
    return '{DBTransaction: AE, invalid}';
  } else {
    return '{DBTransaction: AE}';
  }
} # debug_info

sub DESTROY {
  if (defined $_[0]->{db}) {
    $_[0]->rollback;
    die "Transaction is rollbacked since it is not explicitly committed",
        Carp::shortmess();
  }

  local $@;
  eval { die };
  warn "$$: Reference to " . $_[0]->debug_info . " is not discarded before global destruction\n"
      if $@ =~ /during global destruction/;
} # DESTROY

package Dongry::Database::Transaction;
our $VERSION = '1.0';
use Carp;

push our @CARP_NOT, qw(Dongry::Database);

sub commit ($;%) {
  if ($_[0]->{db}->{in_transaction}) {
    $_[0]->{db}->{dbhs}->{master}->commit;
    delete $_[0]->{db}->{in_transaction};
  } else {
    croak "This transaction can no longer be committed";
  }
} # commit

sub rollback ($;%) {
  if ($_[0]->{db}->{in_transaction}) {
    $_[0]->{db}->{dbhs}->{master}->rollback;
    delete $_[0]->{db}->{in_transaction};
  } else {
    croak "This transaction can no longer be rollbacked";
  }
} # rollback

sub debug_info ($) {
  return '{DBTransaction}';
} # debug_info

sub DESTROY {
  if ($_[0]->{db}->{in_transaction}) {
    $_[0]->rollback;
    die "Transaction is rollbacked since it is not explicitly committed",
        Carp::shortmess();
  }

  local $@;
  eval { die };
  warn "$$: Reference to " . $_[0]->debug_info . " is not discarded before global destruction\n"
      if $@ =~ /during global destruction/;
} # DESTROY

# ------ Source selection ------

package Dongry::Database::ForceSource;
our $VERSION = '1.0';

sub end {
  if ($_[0]->{db}->{force_source_name} and
      $_[0]->{db}->{force_source_name} eq $_[0]->{source_name}) {
    delete $_[0]->{db}->{force_source_name};
  }
} # end

sub debug_info ($) {
  return sprintf '{DBForceSource: %s}', $_[0]->{source_name};
} # debug_info

sub DESTROY {
  $_[0]->end;

  local $@;
  eval { die };
  warn "$$: Reference to " . $_[0]->debug_info . " is not discarded before global destruction\n"
      if $@ =~ /during global destruction/;
} # DESTROY

# ------ Dummy objects ------

package Dongry::Database::BrokenConnection;
our $VERSION = '2.0';

sub ping {
  require AnyEvent::MySQL::Client::Promise;
  return AnyEvent::MySQL::Client::Promise->resolve (0);
}

sub query {
  require AnyEvent::MySQL::Client::Promise;
  return AnyEvent::MySQL::Client::Promise->reject ("No connection ($_[0]->{error_text})");
}
sub statement_prepare {
  require AnyEvent::MySQL::Client::Promise;
  return AnyEvent::MySQL::Client::Promise->reject ("No connection ($_[0]->{error_text})");
}
sub statement_execute {
  require AnyEvent::MySQL::Client::Promise;
  return AnyEvent::MySQL::Client::Promise->reject ("No connection ($_[0]->{error_text})");
}
sub statement_close {
  require AnyEvent::MySQL::Client::Promise;
  return AnyEvent::MySQL::Client::Promise->reject ("No connection ($_[0]->{error_text})");
}
sub statement_reset {
  require AnyEvent::MySQL::Client::Promise;
  return AnyEvent::MySQL::Client::Promise->reject ("No connection ($_[0]->{error_text})");
}

sub disconnect {
  require AnyEvent::MySQL::Client::Promise;
  return AnyEvent::MySQL::Client::Promise->resolve;
}

# ------ Loader ------

package Dongry::Database::Registry;
our $VERSION = '1.0';

push our @CARP_NOT, qw(Dongry::Database);

sub load ($$) {
  local $Dongry::Database::Registry = $_[0]->{Registry};
  local $Dongry::Database::Instances = $_[0]->{Instances};
  return Dongry::Database->load($_[1]);
} # load

1;

=head1 LICENSE

Copyright 2011-2023 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
