package Dongry::Database;
use strict;
use warnings;
our $VERSION = '2.0';
use DBI;
use Carp;
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
  Dongry::Database::Transaction Dongry::Database::ForceSource
  Dongry::Table Dongry::Table::Row Dongry::Query
  Dongry::SQL
  Dongry::Database::BrokenConnection
  AnyEvent::DBI AnyEvent::DBI::Hashref AnyEvent::DBI::Carp
);

our $ListClass ||= 'List::Ish';

sub _list {
  eval qq{ require $ListClass } or die $@;
  return $ListClass->new ($_[1] || []);
} # _list

our $SQLDebugClass ||= 'DBIx::ShowSQL';

if ($ENV{SQL_DEBUG}) {
  eval qq{ require $SQLDebugClass } or die $@;
}

# ------ Construction ------

sub new ($;%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

our $Registry ||= {};
our $Instances = {};

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
           ($def->{get_schema} or sub { undef })->());
} # load

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
  return scalar Carp::caller_info
      (Carp::short_error_loc || Carp::long_error_loc);
} # _get_caller

sub connect ($$) {
  my $self = shift;
  my $name = shift or croak 'No data source name';
  return if $self->{dbhs}->{$name};
  my $source = $self->{sources}->{$name}
      or croak "Data source |$name| is not defined";

  if ($source->{anyevent}) {
    require AnyEvent::DBI::Carp;
    require AnyEvent::DBI::Hashref;

    my $onerror_args = {db => $self, caller => _get_caller};
    weaken $onerror_args->{db};
    $self->{dbhs}->{$name} = AnyEvent::DBI::Carp->new
        ($source->{dsn}, $source->{username}, $source->{password},
         on_error => sub {
           #my ($dbh, $filename, $line, $fatal) = @_;

           ## Please note that there are |local $@|s outside of this
           ## callback within the call stack (specifically, in
           ## AnyEvent::DBI), such that |die|s might not work as
           ## intended.

           my $error_text = $@;
           my $file_name = $_[1];
           my $line = $_[2];
           if ($_[2] == $onerror_args->{connect_line}) {
             $file_name = $onerror_args->{caller}->{file};
             $line = $onerror_args->{caller}->{line};
           }

           if ($_[3]) { # fatal
             ## Remove remaining pointer to the broken AnyEvent::DBI
             ## object.
             $onerror_args->{db}->{dbhs}->{$name} = bless {
               error_text => $error_text,
             }, 'Dongry::Database::BrokenConnection';
           }

           eval {
             local $Carp::CarpLevel = $Carp::CarpLevel + 1; 
             $onerror_args->{db}->onerror
                 ->($onerror_args->{db},
                    anyevent => 1,
                    text => $error_text,
                    file_name => $file_name,
                    line => $line,
                    source_name => $name,
                    sql => $onerror_args->{db}->{last_sql});
             1;
           } or do {
             warn "Died within the |onerror| handler: $@";
           };
           
           ## Don't |die| here.  |die| does not work well.  Just
           ## continue and leave the error handling for the
           ## application.
         }, # on_error
         RaiseError => 0, PrintError => 0);
    $onerror_args->{connect_line} = __LINE__ - 1;
  } else {
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
          }, AutoCommit => 1, ReadOnly => !$source->{writable}});
  }
  $self->onconnect->($self, source_name => $name);
} # connect

sub disconnect ($$) {
  my $self = shift;
  for my $name (
    @_ ? ($_[0]) : (keys %{$self->{sources} or {}})
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
    
    if ($self->{dbhs}->{$name}) {
      $self->{dbhs}->{$name}->disconnect
          if $self->{dbhs}->{$name}->can ('disconnect');
      delete $self->{dbhs}->{$name};
    }
  }
} # disconnect

sub DESTROY {
  $_[0]->disconnect;
} # DESTROY

# ------ Transaction and source selection ------

sub transaction ($;%) {
  my ($self, %args) = @_;
  croak "Can't start new transaction before committing the current transaction"
      if $self->{in_transaction};
  croak "Can't start new transaction while a source is forced"
      if $self->{force_source_name};

  $self->connect ('master');
  $self->{in_transaction} = 1;
  if ($self->{sources}->{master}->{anyevent}) {
    weaken ($self = $self);
    $self->{dbhs}->{master}->begin_work (sub {
      if ($_[1]) {
        if ($args{cb}) {
          $args{cb}->($self);
        }
      } else {
        if ($args{onerror}) {
          $args{onerror}->($self);
        }
      }
    });
    return bless {db => $self}, 'Dongry::Database::Transaction::AnyEvent';
  } else {
    $self->{dbhs}->{master}->begin_work ($args{cb});
    return bless {db => $self}, 'Dongry::Database::Transaction';
  }
} # transaction

sub force_source_name ($$) {
  my $self = shift;
  croak "Can't force source in a transaction" if $self->{in_transaction};
  croak "Can't force source while another source is forced"
      if $self->{force_source_name};

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

sub execute ($$;$%) {
  my ($self, $sql, $values, %args) = @_;

  my $name = $args{source_name} ||
      ($self->{force_source_name}
         ? $self->{force_source_name}
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

  $self->connect ($name);

  if (not $self->{sources}->{$name}->{writable} and
      ($args{must_be_writable} or
       (not $args{even_if_read_only} and
        not $sql =~ /$ReadOnlyQueryPattern/o))) {
    croak "Data source |$name| is read-only";
  }

  if (defined $values and ref $values eq 'HASH') {
    ($sql, $values) = _where [$sql, %$values];
  }
  
  if ($EmbedCallerInSQL) {
    my $caller = _get_caller;
    my $text = $name . ' at ' . $caller->{file} . ' line ' . $caller->{line};
    $text =~ s{\*/}{\* /}g;
    $sql .= ' /* ' . $text . ' */';
  }

  if ($self->{sources}->{$name}->{anyevent}) {
    weaken ($self = $self);
    $self->{dbhs}->{$name}->exec_or_fatal_as_hashref
        ($sql, @{$values or []}, sub {
           $self->{last_sql} = $sql;
           if ($#_) {
             if ($args{cb}) {
               my $result = bless {db => $self,
                                   data => $_[1],
                                   row_count => $_[2]},
                   'Dongry::Database::Executed::Inserted';
               $args{cb}->($self, $result);
             }
           } else {
             if ($args{cb}) {
               my $result = bless {error_text => $@, error_sql => $sql},
                   'Dongry::Database::Executed::NotAvailable';
               eval {
                 $args{cb}->($self, $result);
                 1;
               } or do {
                 ## Because of |local $@| in AnyEvent::DBI, any
                 ## exception thrown in the callback will be ate by
                 ## it.  Therefore the callback should not throw an
                 ## exception.  We catch any exception and then
                 ## rethrow here such that unintentional exceptions
                 ## (e.g. method not found error) can be warned here.
                 ## Applications should not rely on this behavior for
                 ## purposes other than development.
                 warn $@;
                 die $@;
               };
             }
           }
         });
    return bless {}, 'Dongry::Database::Executed::NotAvailable'
        if defined wantarray;
  } else {
    my $sth = $self->{dbhs}->{$name}->prepare ($self->{last_sql} = $sql);
    my $rows = $sth->execute (@{$values or []});
    return if not defined wantarray and not $args{cb};

    my $result = bless {db => $self, sth => $sth, row_count => $rows},
        'Dongry::Database::Executed';
    if ($args{cb}) {
      local $Carp::CarpLevel = $Carp::CarpLevel + 1;
      $args{cb}->($self, $result);
    }
    return $result;
  }
} # execute

# ------ Structured SQL executions ------

sub set_tz ($;$%) {
  my ($self, $tz, %args) = @_;
  $tz ||= '+00:00';
  $self->execute ('SET time_zone = ?', [$tz],
                  source_name => $args{source_name},
                  even_if_read_only => 1,
                  cb => $args{cb});
  return undef;
} # set_tz

sub insert ($$$;%) {
  my ($self, $table_name, $data, %args) = @_;

  croak "No data" unless @$data;
  my %col;
  for (@$data) {
    $col{$_} = 1 for keys %$_;
  }

  my @col = keys %col;
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
  }
  
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
    my @col = keys %$value;
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
  }

  my $cb = $args{cb};
  if ($cb) {
    my $cb_orig = $cb;
    $cb = sub {
      unless ($_[1]->is_error) {
        bless $_[1], 'Dongry::Database::Executed::Inserted';
        $_[1]->{table_name} = $table_name;
        $_[1]->{data} = $data;
      }
      goto &$cb_orig;
    }; # $cb
  }

  my $return = $self->execute
      ($sql, \@values, source_name => $args{source_name}, cb => $cb);

  return unless defined wantarray;
  return $return if $return->is_error;
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
    carp "Lock used outside of transaction" unless $self->{in_transaction};
    $sql .= ' FOR UPDATE' if $args{lock} eq 'update';
    $sql .= ' LOCK IN SHARE MODE' if $args{lock} eq 'share';
    $args{must_be_writable} = 1;
  }

  my $cb = $args{cb};
  if ($cb) {
    my $cb_orig = $cb;
    $cb = sub {
      $_[1]->{table_name} = $table_name unless $_[1]->is_error;
      goto &$cb_orig;
    }; # $cb
  }

  my $return = $self->execute
      ($sql, $where_bind,
       source_name => $args{source_name},
       must_be_writable => $args{must_be_writable},
       cb => $cb);
  return unless defined wantarray;

  $return->{table_name} = $table_name unless $return->is_error;
  return $return;
} # select

sub update ($$$%) {
  my ($self, $table_name, $value, %args) = @_;
  
  my @col = keys %$value;
  croak 'No value to update' unless @col;

  my ($where_sql, $where_bind) = _where ($args{where});
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
      cb => $args{cb});
} # update

sub delete ($$$;%) {
  my ($self, $table_name, $where, %args) = @_;

  my ($where_sql, $where_bind) = _where ($where);
  croak 'No where' unless $where_sql;
  
  my $sql = 'DELETE FROM ' . (_quote $table_name) . ' WHERE ' . $where_sql;
  $sql .= ' ORDER BY ' . _order ($args{order}) if $args{order};
  croak 'Offset is not supported' if defined $args{offset};
  $sql .= sprintf ' LIMIT %d', $args{limit} || 1 if defined $args{limit};

  return $self->execute
      ($sql, $where_bind, source_name => $args{source_name},
       cb => $args{cb});
} # delete

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
  my $list = $_[0]->{db}->_list ($sth->fetchall_arrayref ({}));
  $sth->finish;
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

sub debug_info ($) {
  my $self = shift;
  my @info;
  push @info, 'error' if $self->is_error;
  for my $name (qw(table_name error_text error_sql)) {
    my $v = $self->$name;
    push @info, $name . ' = ' . $v if defined $v;
  }
  return sprintf '{DBExecuted: %s}', join '; ', @info;
} # debug_info

sub DESTROY {
  $_[0]->{sth}->finish if $_[0]->{sth};
} # DESTROY

package Dongry::Database::Executed::Inserted;
our $VERSION = '1.0';
push our @ISA, 'Dongry::Database::Executed';
use Carp;

sub each ($$) {
  my ($self, $code) = @_;
  my $data = delete $self->{data}
      or croak 'This method is no longer available';
  $code->() for @$data;
  delete $self->{data};
} # each

sub each_as_row ($$) {
  my ($self, $code) = @_;
  my $tn = $self->{table_name};
  croak 'Table name is not known' if not defined $tn;
  my $data = delete $self->{data}
      or croak 'This method is no longer available';
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

# ------ Transaction ------

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
        Carp::shortmess;
  }
  if ($Dongry::LeakTest) {
    warn "Possible memory leak by object " . ref $_[0];
  }
} # DESTROY

package Dongry::Database::Transaction::AnyEvent;
our $VERSION = '1.0';
push our @ISA, qw(Dongry::Database::Transaction);
use Scalar::Util qw(weaken);
use Carp;

## Please note that the |in_transaction| flag does not sync with the
## actual state of the transaction in the async mode.

sub commit ($;%) {
  my ($self, %args) = @_;
  if ($self->{db}->{in_transaction}) {
    weaken ($self = $self);
    $self->{db}->{dbhs}->{master}->commit (sub {
      if ($#_ || !$@) { ## AnyEvent::DBI documentation is wrong...
        if ($args{cb}) {
          my $result = bless {}, 'Dongry::Database::Executed::Inserted';
          $args{cb}->($self->{db}, $result);
        }
      } else {
        if ($args{cb}) {
          my $result = bless {error_text => $@, error_sql => 'commit'},
              'Dongry::Database::Executed::NotAvailable';
          eval {
            $args{cb}->($self->{db}, $result);
            1;
          } or do {
            ## See note for |execute|.
            warn $@;
            die $@;
          };
        }
      }
    });
    delete $self->{db}->{in_transaction};
  } else {
    croak "This transaction can no longer be committed";
  }
} # commit

sub rollback ($;%) {
  my ($self, %args) = @_;
  if ($self->{db}->{in_transaction}) {
    weaken ($self = $self);
    $self->{db}->{dbhs}->{master}->rollback (sub {
      if ($#_ || !$@) { ## AnyEvent::DBI documentation is wrong...
        if ($args{cb}) {
          my $result = bless {}, 'Dongry::Database::Executed::Inserted';
          $args{cb}->($self->{db}, $result);
        }
      } else {
        if ($args{cb}) {
          my $result = bless {error_text => $@, error_sql => 'rollback'},
              'Dongry::Database::Executed::NotAvailable';
          eval {
            $args{cb}->($self->{db}, $result);
            1;
          } or do {
            ## See note for AnyEvent::DBI.
            warn $@;
            die $@;
          };
        }
      }
    });
    delete $self->{db}->{in_transaction};
  } else {
    croak "This transaction can no longer be rollbacked";
  }
} # rollback

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
  if ($Dongry::LeakTest) {
    warn "Possible memory leak by object " . ref $_[0];
  }
} # DESTROY

# ------ Dummy objects ------

package Dongry::Database::BrokenConnection;
our $VERSION = '1.0';

sub exec_or_fatal_as_hashref {
  my $cb = pop;
  local $@ = $_[0]->{error_text};
  $cb->($_[0]);
} # exec_as_hashref

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
