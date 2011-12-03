package Dongry::Database;
use strict;
use warnings;
our $VERSION = '1.0';
use DBI;
use Carp;

push our @CARP_NOT, qw(
  DBI DBI::st DBI::db
  Dongry::Database::Executed Dongry::Database::Executed::Inserted
  Dongry::Database::Transaction 
  Dongry::Table Dongry::Table::Row Dongry::Query
);

# ------ Construction ------

sub new ($;%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

our $Registry ||= {};
our $Instances = {};

sub load ($$) {
  return $Instances->{$_[1]} if $Instances->{$_[1]};
  
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
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
} # onerror

sub onerror ($) {
  if (@_ > 1) {
    $_[0]->{onerror} = $_[1];
  }
  return $_[0]->{onerror} || sub {
    local $Carp::CarpLevel = $Carp::CarpLevel - 1; # Bogus hack (Don't copy!)
    my ($self, %args) = @_;
    croak $self->source ($args{source_name})->{dsn} .
        ': ' . $args{text} .
        (defined $args{sql} ? ': ' . $args{sql} : '');
  };
} # onerror

sub connect ($$) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  my $name = shift or croak 'No data source name';
  return if $self->{dbhs}->{$name};
  my $source = $self->{sources}->{$name}
      or croak "Data source |$name| is not defined";

  $self->{dbhs}->{$name} = DBI->connect
      ($source->{dsn}, $source->{username}, $source->{password},
       {RaiseError => 1, PrintError => 0, HandleError => sub {
          #my ($msg, $dbh, $returned) = @_:
          local $Carp::CarpLevel = $Carp::CarpLevel + 2;
          $self->onerror->($self,
                           text => $_[0],
                           source_name => $name,
                           sql => $self->{last_sql});
          return 0;
        }, AutoCommit => 1, ReadOnly => !$source->{writable}});
  {
    #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
    $self->onconnect->($self, source_name => $name);
  }
} # connect

sub disconnect ($$) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

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
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  croak "$self->{sources}->{master}->{dsn}: Cannot start new transaction before committing the current transaction"
      if $self->{in_transaction};

  $self->connect ('master');
  $self->{in_transaction} = 1;
  $self->{dbhs}->{master}->begin_work;
  return bless {db => $self}, 'Dongry::Database::Transaction';
} # transaction

# ------ SQL Execution ------

our $ReadOnlyQueryPattern = qr/^\s*(?:[Ss][Ee][Ll][Ee][Cc][Tt]|[Ss][Hh][Oo][Ww]|[Dd][Ee][Ss][Cc]|[Ee][Xx][Pp][Ll][Aa][Ii][Nn])\b/;

sub execute ($$;$%) {
  my ($self, $sql, $values, %args) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

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

sub insert ($$$;%) {
  my ($self, $table_name, $data, %args) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  my %col;
  for (@$data) {
    $col{$_} = 1 for keys %$_;
  }

  my @col = keys %col;
  my @values = map {
    my $data = $_;
    map { $data->{$_} } @col;
  } @$data;

  my $placeholder = join ', ', ('?') x @col;
  
  my $sql = 'INSERT INTO ' . $table_name .
      ' (' . (join ', ', @col) . ')' .
      ' VALUES ' .
        (join ', ', ("($placeholder)") x @$data) .
      '';
  my $return = $self->execute ($sql, \@values, source_name => $args{source_name});

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

sub _where ($$) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  if (ref $_[0] eq 'HASH') {
    require SQL::Abstract;
    $self->{sqla} ||= SQL::Abstract->new;
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
  if (defined $_[1] and ref $_[1] eq 'ARRAY' and @{$_[1]}) {
    my @s;
    for (0..int ($#{$_[1]} / 2)) {
      push @s, $_[1]->[$_ * 2] . ' ' . ($_[1]->[$_ * 2 + 1] || 'ASC');
    }
    return ' ORDER BY ' . join ', ', @s;
  } else {
    return '';
  }
} # _order

sub select ($$$;%) {
  my ($self, $table_name, $where, %args) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  my ($where_sql, $where_bind) = $self->_where ($where);
  croak 'No where' unless $where_sql;
  
  my $sql = 'SELECT ';
  $sql .= $args{field} || '*';
  $sql .= ' FROM ' . $table_name . $where_sql;
  $sql .= $self->_order ($args{order});
  $sql .= ' LIMIT ' . ($args{offset} || 0) . ',' . ($args{limit} || 1)
      if $args{limit} or $args{offset};
  if ($args{lock}) {
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
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  
  my @col = keys %$value;
  croak 'No value to update' unless @col;
  my @value = map { $value->{$_} } @col;

  my ($where_sql, $where_bind) = $self->_where ($where);
  croak 'No where' unless $where_sql;

  my $sql = sprintf 'UPDATE %s' .
      ' SET ' . (join ', ', ('%s = ?') x @col) .
      $where_sql,
      $table_name, @col;
  my $return = $self->execute ($sql, [@value, @$where_bind]);

  return unless defined wantarray;
  bless $return, 'Dongry::Database::Executed';
  $return->{table_name} = $table_name;
  return $return;
} # update

sub delete ($$$;%) {
  my ($self, $table_name, $where, %args) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  my ($where_sql, $where_bind) = $self->_where ($where);
  croak 'No where' unless $where_sql;
  
  my $sql = 'DELETE FROM ' . $table_name . $where_sql;
  my $return = $self->execute
      ($sql, $where_bind, source_name => $args{source_name});

  return unless defined wantarray;
  bless $return, 'Dongry::Database::Executed';
  $return->{table_name} = $table_name;
  return $return;
} # delete

# ------ Accessors to more abstract interfaces ------

$Dongry::Types ||= {};

$Dongry::Types->{as_ref} = {
  parse => sub {
    return \($_[0]);
  },
  serialize => sub {
    return ${$_[0]};
  },
}; # as_ref

sub schema ($) {
  if (@_ > 1) {
    $_[0]->{schema} = $_[1];
  }
  return $_[0]->{schema};
} # schema

sub table ($$) {
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

package Dongry::Database::Executed;
our $VERSION = '1.0';
use Carp;
use List::Rubyish;

push our @CARP_NOT, qw(Dongry::Database List::Rubyish);

sub row_count ($) {
  return $_[0]->{row_count};
} # row_count

sub table_name ($) {
  if (@_ > 1) {
    $_[0]->{table_name} = $_[1];
  }
  return $_[0]->{table_name};
} # table_name

sub for_each ($$) {
  my ($self, $code) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  while (my $hashref = $self->{sth}->fetchrow_hashref) {
    #local $Carp::CarpLevel = -2;
    $code->($hashref);
  }
} # for_each

sub for_each_as_row ($$) {
  my ($self, $code) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  my $db = $self->{db};
  my $tn = $self->{table_name} or croak 'Table name is not known';
  require Dongry::Table;
  $self->for_each (sub {
    #local $Carp::CarpLevel = -2;
    my $row = Dongry::Table->new_row
        (db => $db,
         table_name => $tn,
         data => $_[0]);
    $code->($row);
  });
} # for_each_as_row

sub all ($) {
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  return $_[0]->{data}
      ||= List::Rubyish->new ($_[0]->{sth}->fetchall_arrayref ({}));
} # all

sub all_as_rows ($) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  my $db = $self->{db};
  my $tn = $self->{table_name} or croak 'Table name is not known';
  require Dongry::Table;
  return $_[0]->{all_as_rows} ||= $self->all->map(sub {
    return Dongry::Table->new_row
        (db => $db,
         table_name => $tn,
         data => $_);
  });
} # all_as_rows

sub first ($) {
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  return $_[0]->{sth}->fetchrow_hashref; # or undef
} # first

sub first_as_row ($) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  croak 'Table name is not known' unless $self->{table_name};

  my $data = $self->first or return undef;
  require Dongry::Table;
  return Dongry::Table->new_row
      (db => $self->{db},
       table_name => $self->{table_name},
       data => $data);
} # first_as_row

package Dongry::Database::Executed::Inserted;
our $VERSION = '1.0';
push our @ISA, 'Dongry::Database::Executed';
use Carp;

sub for_each ($$;%) {
  my ($self, $code, %args) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 2;
  for (@{$self->{data}}) {
    $code->($_);
  }
} # for_each

sub all ($) {
  return ref $_[0]->{data} eq 'ARRAY'
      ? List::Rubyish->new ($_[0]->{data}) : $_[0]->{data};
} # all

sub all_as_rows ($) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  my $db = $self->{db};
  my $tn = $self->{table_name} or croak 'Table name is not known';
  require Dongry::Table;
  return $_[0]->{all_as_rows} ||= List::Rubyish->new([map {
    return Dongry::Table->new_row
        (db => $db,
         table_name => $tn,
         data => $self->{data}->[$_],
         $self->{parsed_data} ? (parsed_data => $self->{parsed_data}->[$_]) : ());
  } 0..$#{$self->{data}}]);
} # all_as_rows

sub first ($) {
  return $_[0]->{data}->[0]; # or undef
} # first

sub first_as_row ($) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  return undef unless @{$self->{data}};
  croak 'Table name is not known' unless $self->{table_name};

  require Dongry::Table;
  return Dongry::Table->new_row
      (db => $self->{db},
       table_name => $self->{table_name},
       data => $self->{data}->[0],
       ($self->{parsed_data} ? (parsed_data => $self->{parsed_data}->[0]) : ()));
} # first_as_row

package Dongry::Database::Transaction;
our $VERSION = '1.0';
use Carp;

push our @CARP_NOT, qw(Dongry::Database);

sub commit ($) {
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  $_[0]->{db}->{dbhs}->{master}->commit if $_[0]->{db}->{in_transaction};
  delete $_[0]->{db}->{in_transaction};
} # commit

sub rollback ($) {
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  $_[0]->{db}->{dbhs}->{master}->rollback if $_[0]->{db}->{in_transaction};
  delete $_[0]->{db}->{in_transaction};
} # rollback

sub DESTROY {
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  if ($_[0]->{db}->{in_transaction}) {
    $_[0]->rollback;
    carp "Transaction is rollbacked since it is not explicitly committed";
    exit 1;
  }
} # DESTROY

1;
