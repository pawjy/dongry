package Dongry::Database;
use strict;
use warnings;
our $VERSION = '1.0';
use DBI;
use Carp;
use Scalar::Util qw(weaken);

push our @CARP_NOT, qw(
  DBI DBI::st DBI::db
  Dongry::Database::Executed Dongry::Database::Executed::Inserted
  Dongry::Database::Transaction 
  Dongry::Table Dongry::Table::Row Dongry::Query
  SQL::Abstract SQL::NamedPlaceholder
);

if ($ENV{DONGRY_DEBUG}) {
  require DBIx::ShowSQL;
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
    local $Carp::CarpLevel = $Carp::CarpLevel - 1;
    croak $self->source ($args{source_name})->{dsn} .
        ': ' . $args{text} .
        (defined $args{sql} ? ': ' . $args{sql} : '');
  };
} # onerror

sub connect ($$) {
  my $self = shift;
  my $name = shift or croak 'No data source name';
  return if $self->{dbhs}->{$name};
  my $source = $self->{sources}->{$name}
      or croak "Data source |$name| is not defined";

  my $onerror_args = {db => $self};
  weaken $onerror_args->{db};
  $self->{dbhs}->{$name} = DBI->connect
      ($source->{dsn}, $source->{username}, $source->{password},
       {RaiseError => 1, PrintError => 0, HandleError => sub {
          #my ($msg, $dbh, $returned) = @_:
          local $Carp::CarpLevel = $Carp::CarpLevel + 1;
          $onerror_args->{db}->onerror
              ->($onerror_args->{db},
                 text => $_[0],
                 source_name => $name,
                 sql => $onerror_args->{db}->{last_sql});
          return 0;
        }, AutoCommit => 1, ReadOnly => !$source->{writable}});
  {
    $self->onconnect->($self, source_name => $name);
  }
} # connect

sub disconnect ($$) {
  my $self = shift;
  for my $name (
    @_ ? ($_[0]) : (keys %{$self->{sources} or {}})
  ) {
    if ($self->{in_transaction} and $name eq 'master') {
      carp "$self->{sources}->{$name}->{dsn}: A transaction is rollbacked because the database is disconnected before the transaction is committed";
      $self->{dbhs}->{$name}->rollback;
      delete $self->{in_transaction};
    }
    
    if ($self->{dbhs}->{$name}) {
      $self->{dbhs}->{$name}->disconnect;
      delete $self->{dbhs}->{$name};
    }
  }
} # disconnect

sub DESTROY {
  $_[0]->disconnect;
} # DESTROY

# ------ Transaction ------

sub transaction ($) {
  my $self = shift;
  croak "$self->{sources}->{master}->{dsn}: Cannot start new transaction before committing the current transaction"
      if $self->{in_transaction};

  $self->connect ('master');
  $self->{in_transaction} = 1;
  $self->{dbhs}->{master}->begin_work;
  return bless {db => $self}, 'Dongry::Database::Transaction';
} # transaction

# ------ Bare SQL operations ------

our $ReadOnlyQueryPattern = qr/^\s*(?:
  [Ss][Ee][Ll][Ee][Cc][Tt]|
  [Ss][Hh][Oo][Ww]|
  [Dd][Ee][Ss][Cc](?:[Rr][Ii][Bb][Ee])?|
  [Ee][Xx][Pp][Ll][Aa][Ii][Nn]
)\b/x;

sub execute ($$;$%) {
  my ($self, $sql, $values, %args) = @_;

  my $name = $args{source_name} ||
      ((!$self->{in_transaction} &&
        !$args{must_be_writable} &&
        $sql =~ /$ReadOnlyQueryPattern/o)
           ? 'default' : 'master');
  if ($name ne 'master' and $self->{in_transaction}) {
    croak "Data source |$name| cannot be used in transaction";
  }

  $self->connect ($name);

  if (not $self->{sources}->{$name}->{writable} and
      ($args{must_be_writable} or
       (not $args{even_if_read_only} and
        not $sql =~ /$ReadOnlyQueryPattern/o))) {
    croak "Data source |$name| is read-only";
  }

  my $sth = $self->{dbhs}->{$name}->prepare ($self->{last_sql} = $sql);
  my $rows = $sth->execute (@{$values or []});
  return unless defined wantarray;

  return bless {db => $self, sth => $sth, row_count => $rows},
      'Dongry::Database::Executed';
} # execute

# ------ Structured SQL executions ------

sub set_tz ($;$%) {
  my ($self, $tz, %args) = @_;
  $tz ||= '+00:00';
  $self->execute ('SET time_zone = ?', [$tz],
                  source_name => $args{source_name},
                  even_if_read_only => 1);
} # set_tz

## <http://dev.mysql.com/doc/refman/5.6/en/identifiers.html>.
sub _quote ($) {
  my $s = $_[0];
  $s =~ s/`/``/g;
  return q<`> . $s . q<`>;
} # _quote

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
    push @values, (map { exists $data->{$_} ? ($data->{$_}) : () } @col);
    push @placeholders, 
        '(' . (join ', ', (map { exists $data->{$_}
                                     ? '?' : 'DEFAULT' } @col)) . ')';
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
          ref $value->{$_} eq 'Dongry::Database::BareSQLFragment') {
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

  my $return = $self->execute
      ($sql, \@values, source_name => $args{source_name});

  return unless defined wantarray;
  bless $return, 'Dongry::Database::Executed::Inserted';
  $return->{table_name} = $table_name;
  $return->{data} = $data;
  return $return;
} # insert

sub last_insert_id ($) {
  my $dbh = $_[0]->{dbhs}->{master} or return undef;
  return $dbh->last_insert_id (undef, undef, undef, undef);
} # last_insert_id

sub _fields ($);
sub _fields ($) {
  if (not defined $_[0]) {
    return '*';
  } elsif (not ref $_[0]) {
    return _quote $_[0];
  } elsif (ref $_[0] eq 'ARRAY') {
    if (@{$_[0]}) {
      return join ', ', map { _fields ($_) } @{$_[0]};
    } else {
      croak 'Array reference cannot be empty';
    }
  } elsif (ref $_[0] eq 'HASH') {
    my $func = [grep { /^-/ } keys %{$_[0]}]->[0] || '';
    if ($func =~ /\A-(count|min|max|sum)\z/) {
      my $v = (uc $1) . '(';
      $v .= 'DISTINCT ' if $_[0]->{distinct};
      $v .= _fields ($_[0]->{$func});
      $v .= ')';
      $v .= ' AS ' . _quote $_[0]->{as} if defined $_[0]->{as};
      return $v;
    } else {
      if ($func) {
        croak sprintf 'Field function %s is not supported', $func;
      } else {
        croak 'Hash reference must contain a field function name';
      }
    }
  } elsif (ref $_[0] eq 'Dongry::Database::BareSQLFragment') {
    return ${$_[0]};
  } else {
    croak sprintf 'Field value %s is not supported', $_[0];
  }
} # _fields

sub _where ($$) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  if (ref $_[0] eq 'HASH') {
    require SQL::Abstract;
    $self->{sqla} ||= SQL::Abstract->new (quote_char => q{`});
    my ($sql, @bind) = $self->{sqla}->where ($_[0]);
    return ($sql, \@bind);
  } elsif (ref $_[0] eq 'ARRAY') {
    require SQL::NamedPlaceholder;
    my $where = $_[0];
    return SQL::NamedPlaceholder::bind_named
        (' WHERE ' . $where->[0], {@{$where}[1..$#$where]});
  } else {
    croak 'Where parameter is broken';
  }
} # _where

sub _order ($$) {
  if (defined $_[1]) {
    my @s;
    for (0..(int (($#{$_[1]} + 2) / 2) - 1)) {
      push @s, (_quote $_[1]->[$_ * 2]) . ' ' . (
        {
          'ASC' => 'ASC',
          'asc' => 'ASC',
          '1' => 'ASC',
          '+1' => 'ASC',
          'DESC' => 'DESC',
          'desc' => 'DESC',
          '-1' => 'DESC',
        }->{$_[1]->[$_ * 2 + 1] || 'ASC'} or
        (croak sprintf 'Unknown order: %s', $_[1]->[$_ * 2 + 1])
      );
    }
    return ' ORDER BY ' . join ', ', @s;
  } else {
    return '';
  }
} # _order

sub select ($$$;%) {
  my ($self, $table_name, $where, %args) = @_;

  my ($where_sql, $where_bind) = $self->_where ($where);
  croak 'No where' unless $where_sql;
  
  my $sql = 'SELECT';
  $sql .= ' DISTINCT' if $args{distinct};
  if ($args{fields}) {
    $sql .= ' ' . _fields $args{fields};
  } else {
    $sql .= ' *';
  }
  $sql .= ' FROM ' . (_quote $table_name)
       . $where_sql;
  if ($args{group}) {
    $sql .= ' GROUP BY ' . join ', ', map { _quote $_ } @{$args{group}};
  }
  $sql .= $self->_order ($args{order}) if $args{order};
  $sql .= sprintf ' LIMIT %d,%d', ($args{offset} || 0), ($args{limit} || 1)
      if defined $args{limit} or defined $args{offset};
  if ($args{lock}) {
    carp "Lock used outside of transaction" unless $self->{in_transaction};
    $sql .= ' FOR UPDATE' if $args{lock} eq 'update';
    $sql .= ' LOCK IN SHARE MODE' if $args{lock} eq 'share';
    $args{must_be_writable} = 1;
  }
  my $return = $self->execute
      ($sql, $where_bind,
       source_name => $args{source_name},
       must_be_writable => $args{must_be_writable});

  bless $return, 'Dongry::Database::Executed';
  $return->{table_name} = $table_name;
  return $return;
} # select

sub update ($$$$;%) {
  my ($self, $table_name, $value, $where, %args) = @_;
  
  my @col = keys %$value;
  croak 'No value to update' unless @col;

  my ($where_sql, $where_bind) = $self->_where ($where);
  croak 'No where' unless $where_sql;

  my $sql .= 'UPDATE';
  $sql .= ' IGNORE' if $args{duplicate} and $args{duplicate} eq 'ignore';
  $sql .= ' ' . _quote $table_name;

  my @sql_value;
  my @bound_value;
  for (@col) {
    if (defined $value->{$_} and
        ref $value->{$_} eq 'Dongry::Database::BareSQLFragment') {
      push @sql_value, (_quote $_), ${$value->{$_}};
    } else {
      push @sql_value, (_quote $_), '?';
      push @bound_value, $value->{$_};
    }
  }
  $sql .= sprintf ' SET ' . (join ', ', ('%s = %s') x (@sql_value / 2)),
      @sql_value;

  $sql .= $where_sql;
  $sql .= $self->_order ($args{order}) if $args{order};
  croak 'Offset is not supported' if defined $args{offset};
  $sql .= sprintf ' LIMIT %d', $args{limit} || 1 if defined $args{limit};
  my $return = $self->execute
     ($sql, [@bound_value, @$where_bind], source_name => $args{source_name});

  return unless defined wantarray;
  bless $return, 'Dongry::Database::Executed';
  $return->{table_name} = $table_name;
  return $return;
} # update

sub delete ($$$;%) {
  my ($self, $table_name, $where, %args) = @_;

  my ($where_sql, $where_bind) = $self->_where ($where);
  croak 'No where' unless $where_sql;
  
  my $sql = 'DELETE FROM ' . (_quote $table_name) . $where_sql;
  $sql .= $self->_order ($args{order}) if $args{order};
  croak 'Offset is not supported' if defined $args{offset};
  $sql .= sprintf ' LIMIT %d', $args{limit} || 1 if defined $args{limit};
  my $return = $self->execute
      ($sql, $where_bind, source_name => $args{source_name});

  return unless defined wantarray;
  bless $return, 'Dongry::Database::Executed';
  $return->{table_name} = $table_name;
  return $return;
} # delete

sub bare_sql_fragment ($$) {
  return bless \($_[1]), 'Dongry::Database::BareSQLFragment';
} # bare_sql_fragment

# ------ Schema-aware operations ------

$Dongry::Types ||= {};

$Dongry::Types->{as_ref} = {
  parse => sub {
    return defined $_[0] ? \($_[0]) : undef;
  },
  serialize => sub {
    return defined $_[0] ? ${$_[0]} : undef;
  },
}; # as_ref

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
      (db => $_[0], name => $_[1]);
} # table

sub query ($%) {
  my ($self, %args) = @_;
  my $query_class = $args{query_class} || 'Dongry::Query';
  eval qq{ require $query_class } or die $@;
  return $query_class->new (db => $self, %args);
} # query

# ------ Operation results ------

package Dongry::Database::Executed;
our $VERSION = '1.0';
use Carp;
use List::Rubyish;

push our @CARP_NOT, qw(Dongry::Database List::Rubyish);

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
  my $tn = $self->{table_name} or croak 'Table name is not known';
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
  my $list = List::Rubyish->new ($sth->fetchall_arrayref ({}));
  $sth->finish;
  return $list;
} # all

sub all_as_rows ($) {
  my $tn = $_[0]->{table_name} or croak 'Table name is not known';
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
  croak 'Table name is not known' unless $self->{table_name};
  my $data = $self->first or return undef;
  require Dongry::Table;
  return bless {db => $self->{db},
                table_name => $self->{table_name},
                data => $data}, 'Dongry::Table::Row';
} # first_as_row

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
  my $tn = $self->{table_name} or croak 'Table name is not known';
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
  return ref $data eq 'ARRAY' ? List::Rubyish->new ($data) : $data;
} # all

sub all_as_rows ($) {
  my $self = shift;
  my $tn = $self->{table_name} or croak 'Table name is not known';
  my $data = delete $self->{data}
      or croak 'This method is no longer available';
  delete $self->{data};
  my $db = $self->{db};
  require Dongry::Table;
  return List::Rubyish->new([map {
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
  croak 'Table name is not known' unless $self->{table_name};
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

# ------ Transaction ------

package Dongry::Database::Transaction;
our $VERSION = '1.0';
use Carp;

push our @CARP_NOT, qw(Dongry::Database);

sub commit ($) {
  if ($_[0]->{db}->{in_transaction}) {
    $_[0]->{db}->{dbhs}->{master}->commit;
    delete $_[0]->{db}->{in_transaction};
  } else {
    croak "This transaction can no longer be committed";
  }
} # commit

sub rollback ($) {
  if ($_[0]->{db}->{in_transaction}) {
    $_[0]->{db}->{dbhs}->{master}->rollback;
    delete $_[0]->{db}->{in_transaction};
  } else {
    croak "This transaction can no longer be rollbacked";
  }
} # rollback

sub DESTROY {
  if ($_[0]->{db}->{in_transaction}) {
    $_[0]->rollback;
    die "Transaction is rollbacked since it is not explicitly committed",
        Carp::shortmess;
  }
} # DESTROY

# ------ Bare SQL fragment ------

package Dongry::Database::BareSQLFragment;
our $VERSION = '1.0';

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
