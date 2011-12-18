package test::Dongry::Table::set;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Type::DateTime;
use Encode;

sub new_db (%) {
  my %args = @_;
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}},
       schema => $args{schema});
  for my $name (keys %{$args{schema} || {}}) {
    if ($args{schema}->{$name}->{_create}) {
      $db->execute ($args{schema}->{$name}->{_create});
    }
  }
  return $db;
} # new_db

# ------ |set| ------

sub _set_parsable : Test(1) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => [qw/col2/],
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = DateTime->new (year => 2008, month => 10, day => 3);
  $row->set ({col1 => $date1});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2008-10-03 00:00:00', col2 => 'abc def'};
} # _set_parsable

sub _set_unparsable : Test(1) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => [qw/col2/],
      _create => 'create table table1 (col1 timestamp default 0,
                                       col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col3 => 'abc xyz'});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def', col3 => 'abc xyz'};
} # _set_unparsable

sub _set_unparsable_but_ref : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => [qw/col2/],
      _create => 'create table table1 (col1 timestamp default 0,
                                       col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col3 => \'abc xyz'});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def', col3 => undef};
} # _set_unparsable_but_ref

sub _set_undef : Test(1) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 timestamp default 0, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col2 => undef});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => undef};
} # _set_undef

sub _set_primary_key_value : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int primary key, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col1 => 121});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data, {col1 => 121, col2 => 'abc def'};

  $row->set ({col1 => 123});

  $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data, {col1 => 123, col2 => 'abc def'};

  $row->set ({col2 => 'xyz'});

  $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data, {col1 => 123, col2 => 'xyz'};
} # _set_primary_key_value

sub _set_unknown_type : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_unknown',
      },
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 timestamp default 0, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col2 => 124222});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def'};
} # _set_unknown_type

sub _set_empty_primary_keys : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
      },
      primary_keys => [],
      _create => 'create table table1 (col1 timestamp default 0, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col2 => 124222});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def'};
} # _set_empty_primary_keys

sub _set_no_primary_keys : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col1 timestamp default 0, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col2 => 124222});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def'};
} # _set_no_primary_keys

sub _set_no_schema : Test(2) {
  my $db = new_db schema => undef;
  $db->execute ('create table table1 (col1 timestamp default 0, col2 blob)');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col2 => 124222});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def'};
} # _set_no_schema

sub _set_unknown_column : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int primary key, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col4 => 121});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data, {col1 => 120, col2 => 'abc def'};
} # _set_unknown_column

sub _set_bare_sql_fragment : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int primary key, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col2 => $db->bare_sql_fragment ('col1 * 2')});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data, {col1 => 120, col2 => 240};
} # _set_bare_sql_fragment

sub _set_bare_sql_fragment_parsed : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col1 int primary key, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col2 => $db->bare_sql_fragment ('col1 * 2')});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data, {col1 => 120, col2 => 240};
} # _set_bare_sql_fragment_parsed

sub _set_no_row : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int primary key, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;
  $db->delete ('table1', {col1 => 120});

  dies_here_ok {
    $row->set ({col2 => 'aaa bbb'});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  is $data, undef;
} # _set_no_row

sub _set_multiple_rows : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc xyz")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col2 => 'aaa bbb'});
  };

  eq_or_diff $db->select ('table1', {col1 => {-not => undef}})->all->to_a,
      [{col1 => 120, col2 => 'aaa bbb'}, {col1 => 120, col2 => 'aaa bbb'}];
} # _set_multiple_rows

sub _set_changed_data : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col2 => 'aaa bbb'});

  is $row->get ('col2'), 'aaa bbb';
  is $row->get_bare ('col2'), 'aaa bbb';
} # _set_changed_data

sub _set_changed_data_parsed : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col1 int, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $ref = \'zta qbv ';
  $row->set ({col2 => $ref});

  is $row->get ('col2'), $ref;
  is $row->get_bare ('col2'), 'zta qbv ';
} # _set_changed_data_parsed

sub _set_changed_data_parsed_bare : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col1 int, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col2 => $db->bare_sql_fragment ('col1 * 10')});

  dies_here_ok {
    $row->get ('col2');
  };
  dies_here_ok {
    $row->get_bare ('col2');
  };
} # _set_changed_data_parsed_bare

sub _set_field_not_in_orig : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}},
                         fields => 'col1')->first_as_row;

  $row->set ({col3 => 'hoge'});

  is $row->get ('col3'), 'hoge';
  is $row->get_bare ('col3'), 'hoge';
} # _set_field_not_in_orig

sub _set_not_writable : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');
  $db->source ('master')->{writable} = 0;

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({col2 => 'hoge'});
  };

  is $row->get ('col2'), 'abc def';
  is $row->reload->get ('col2'), 'abc def';
} # _set_not_writable

sub _set_source_name : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');
  $db->source ('master')->{writable} = 0;
  $db->source ('default')->{writable} = 1;

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col2 => 'hoge'}, source_name => 'default');

  is $row->get ('col2'), 'hoge';
  is $row->reload->get ('col2'), 'hoge';
} # _set_source_name

sub _set_empty : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->set ({});
  };

  is $row->get ('col2'), 'abc def';
} # _set_empty

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
