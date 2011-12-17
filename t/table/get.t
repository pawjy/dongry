package test::Dongry::Table::get;
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

# ------ |get| and |get_bare| ------

sub _get_parsable : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is_datetime $date1, '2001-04-01T00:12:50';
  my $date2 = $row->get ('col1');
  is $date2, $date1;

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_parsable

sub _get_not_parsable : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, '2001-04-01 00:12:50';
  ng ref $date1;

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_not_parsable

sub _get_undef : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1
                  (col1 blob default null, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (NULL, "abc def")');

  my $row = $db->select ('table1', {col1 => undef})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, undef;

  my $str1 = $row->get_bare ('col1');
  is $str1, undef;
} # _get_undef

sub _get_broken : Test(3) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1
                  (col1 blob default null, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("xyxz abc ee-11-11", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_ok {
    my $date1 = $row->get ('col1');
  };
  dies_ok {
    my $date2 = $row->get ('col1');
  };

  my $str1 = $row->get_bare ('col1');
  is $str1, 'xyxz abc ee-11-11';
} # _get_broken

sub _get_column_defined_but_not_found : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col3, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col3 => {-not => undef}})->first_as_row;

  dies_ok {
    my $date1 = $row->get ('col1');
  };

  dies_ok {
    my $str1 = $row->get_bare ('col1');
  };
} # _get_column_defined_but_not_found

sub _get_column_not_found : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col3 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col3, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col3 => {-not => undef}})->first_as_row;

  dies_ok {
    $row->get ('col1');
  };

  dies_ok {
    $row->get_bare ('col1');
  };
} # _get_column_not_found

sub _get_column_unknown_type : Test(3) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'as_unknown',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_ok {
    my $date1 = $row->get ('col1');
  };

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_column_unknown_type

sub _get_column_no_schema : Test(4) {
  my $schema = {
    table2 => {
      type => {
        col1 => 'as_unknown',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, '2001-04-01 00:12:50';
  ng ref $date1;

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_column_no_schema

sub _get_data_from_insert : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  
  my $date0 = DateTime->new (year => 2001, month => 4, day => 1);
  my $row = $db->table ('table1')->create
      ({col3 => $date0, col2 => 'abc def'});

  my $date1 = $row->get ('col3');
  is $date1, $date0;
  isa_ok $date1, 'DateTime';

  my $str1 = $row->get_bare ('col3');
  is $str1, '2001-04-01 00:00:00';
  ng ref $str1;
} # _get_data_from_insert

sub _get_bare_sql_fragment : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  
  my $date0 = DateTime->new (year => 2001, month => 4, day => 1);
  my $row = $db->table ('table1')->create
      ({col3 => $date0, col2 => 'abc def'});
  $row->{data}->{col3} = $db->bare_sql_fragment ('NOW()');

  dies_ok {
    my $date1 = $row->get ('col3');
  };

  dies_ok {
    my $str1 = $row->get_bare ('col3');
  };
} # _get_bare_sql_fragment

# ------ |primary_key_bare_values| ------

sub _primary_key_bare_values_multiple_column_key_1 : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col3 col2/],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;

  my $date0 = DateTime->new (year => 2001, month => 4, day => 1);
  my $row = $db->table ('table1')->create
      ({col3 => $date0, col2 => 'abc def'});

  my $pk = $row->primary_key_bare_values;
  eq_or_diff $pk, {col3 => '2001-04-01 00:00:00', col2 => 'abc def'};
} # _primary_key_bare_values_multiple_column_key_1

sub _primary_key_bare_values_multiple_column_key_2 : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col3 col2/],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => '2001-04-01 00:00:00', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'})->first_as_row;

  my $pk = $row->primary_key_bare_values;
  eq_or_diff $pk, {col3 => '2001-04-01 00:00:00', col2 => 'abc def'};
} # _primary_key_bare_values_multiple_column_key_2

sub _primary_key_bare_values_a_key : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col3/],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => '2001-04-01 00:00:00', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'})->first_as_row;

  my $pk = $row->primary_key_bare_values;
  eq_or_diff $pk, {col3 => '2001-04-01 00:00:00'};
} # _primary_key_bare_values_a_key

sub _primary_key_bare_values_undef : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col3/],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 blob, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => undef, col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'})->first_as_row;

  dies_ok {
    my $pk = $row->primary_key_bare_values;
  };
} # _primary_key_bare_values_undef

sub _primary_key_bare_values_missing : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col3/],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 blob, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => 'a e aaaa', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'},
                         fields => ['col2'])->first_as_row;

  dies_ok {
    my $pk = $row->primary_key_bare_values;
  };
} # _primary_key_bare_values_missing

sub _primary_key_bare_values_missing_some : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2 col3/],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 blob, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => 'a e aaaa', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'},
                         fields => ['col2'])->first_as_row;

  dies_ok {
    my $pk = $row->primary_key_bare_values;
  };
} # _primary_key_bare_values_missing_some

sub _primary_key_bare_values_empty_primary_keys : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 blob, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => 'a e aaaa', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'})->first_as_row;

  dies_ok {
    my $pk = $row->primary_key_bare_values;
  };
} # _primary_key_bare_values_empty_primary_keys

sub _primary_key_bare_values_no_primary_keys : Test(1) {
  my $schema = {
    table1 => {
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 blob, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => 'a e aaaa', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'})->first_as_row;

  dies_ok {
    my $pk = $row->primary_key_bare_values;
  };
} # _primary_key_bare_values_no_primary_keys

sub _primary_key_bare_values_no_schema : Test(1) {
  my $schema = {};
  my $db = new_db schema => $schema;
  $db->execute ('create table table1 (col3 blob, col2 blob)');
  $db->insert ('table1', [{col3 => 'a e aaaa', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'})->first_as_row;

  dies_ok {
    my $pk = $row->primary_key_bare_values;
  };
} # _primary_key_bare_values_no_schema

sub _primary_key_bare_values_bare_sql_fragment : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col3/],
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->insert ('table1', [{col3 => '2001-04-01 00:00:00', col2 => 'abc def'}]);

  my $row = $db->select ('table1', {col2 => 'abc def'})->first_as_row;
  $row->{data}->{col3} = $db->bare_sql_fragment ('NOW()');

  dies_ok {
    my $pk = $row->primary_key_bare_values;
  };
} # _primary_key_bare_values_a_key

# ------ |reload| ------

sub _reload_created_reloaded : Test(4) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;

  my $row = $db->table ('table1')->create
      ({col3 => 'a e aaaa', col2 => 'abc def'});
  dies_ok {
    $row->get ('col1');
  };

  my $row2 = $row->reload;
  is $row2, $row;
  ok $row2->get ('col1');
  is $row2->get_bare ('col1'), $row2->get ('col1');
} # _reload_created_reloaded

sub _reload_parsed_values : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aabre")');

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  
  my $v1 = $row->get ('col2');

  $row->reload;

  my $v2 = $row->get ('col2');
  isnt $v2, $v1;
  is $$v2, $$v1;
} # _reload_parsed_values

sub _reload_from_partial : Test(4) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aabre")');

  my $row = $db->select ('table1', ['1 = 1'],
                         fields => 'col2')->first_as_row;
  
  my $v1 = $row->get ('col2');
  dies_ok {
    $row->get ('col3');
  };

  $row->reload;

  my $v2 = $row->get ('col2');
  isnt $v2, $v1;
  is $$v2, $$v1;

  is $row->get ('col3'), 'aabre';
} # _reload_parsed_values

sub _reload_fields : Test(5) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aabre")');

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  
  my $v1 = $row->get ('col2');
  is $$v1, 2444;
  ok $row->get ('col1');

  $row->reload
      (fields => ['col3', $db->bare_sql_fragment ('col2 * 2 as col2')]);

  my $v2 = $row->get ('col2');
  is $$v2, 2444 * 2;
  is $row->get ('col3'), 'aabre';
  dies_ok {
    $row->get ('col1');
  };
} # _reload_fields

sub _reload_row_not_found : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aabre")');

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  $db->execute ('delete from table1');

  dies_ok {
    $row->reload;
  };

  is ${$row->get ('col2')}, 2444;
  is $row->get ('col3'), 'aabre';
} # _reload_row_not_found

sub _reload_row_found_many : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aabre")');

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aa2")');
  $db->execute ('insert into table1 (col2, col3) values (2444, "aa3")');

  dies_ok {
    $row->reload;
  };

  is ${$row->get ('col2')}, 2444;
  is $row->get ('col3'), 'aabre';
} # _reload_row_found_many

sub _reload_preserve_flags : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aabre")');

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  $row->flags->{hoge} = {abc => 243};

  $row->reload;

  eq_or_diff $row->flags->{hoge}, {abc => 243};
} # _reload_preserve_flags

sub _reload_no_pk_values : Test(4) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col2, col3) values (2444, "aabre")');

  my $row = $db->select ('table1', ['1 = 1'],
                         fields => ['col1', 'col3'])->first_as_row;

  dies_ok {
    $row->reload;
  };

  ok $row->get ('col1');
  dies_ok {
    $row->get ('col2');
  };
  is $row->get ('col3'), 'aabre';
} # _reload_no_pk_values

sub _reload_source_name : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db1 = new_db schema => $schema;
  my $db2 = new_db schema => $schema;
  $db1->execute ('insert into table1 (col2, col3) values (2444, "aabre")');
  $db2->execute ('insert into table1 (col2, col3) values (2444, "aa xy")');
  
  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('master'),
                   default => $db2->source ('master')},
       schema => $schema);

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  is $row->get ('col3'), 'aa xy';

  $row->reload (source_name => 'master');

  is $row->get ('col3'), 'aabre';
} # _reload_source_name

sub _reload_bad_source_name : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)',
    },
  };
  my $db1 = new_db schema => $schema;
  my $db2 = new_db schema => $schema;
  $db1->execute ('insert into table1 (col2, col3) values (2444, "aabre")');
  $db2->execute ('insert into table1 (col2, col3) values (2444, "aa xy")');
  
  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('master'),
                   default => $db2->source ('master')},
       schema => $schema);

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  is $row->get ('col3'), 'aa xy';

  dies_ok {
    $row->reload (source_name => 'notmaster');
  };

  is $row->get ('col3'), 'aa xy';
} # _reload_bad_source_name

sub _reload_in_transaction : Test(4) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)
                  engine = InnoDB',
    },
  };
  my $db1 = new_db schema => $schema;
  my $db2 = new_db schema => $schema;
  $db1->execute ('insert into table1 (col2, col3) values (2444, "aabre")');
  $db2->execute ('insert into table1 (col2, col3) values (2444, "aa xy")');
  
  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('master'),
                   default => $db2->source ('master')},
       schema => $schema);

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  is $row->get ('col3'), 'aa xy';

  my $transaction = $db->transaction;

  $row->reload;

  is $row->get ('col3'), 'aabre';

  $row->set ({col3 => 'bbb'});
  
  is $db1->select ('table1', {col3 => 'bbb'})->row_count, 0;
  
  $transaction->commit;

  is $db1->select ('table1', {col3 => 'bbb'})->row_count, 1;
} # _reload_in_transaction

sub _reload_in_transaction_locked : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col2/],
      type => {col2 => 'as_ref'},
      _create => 'create table table1 (col3 blob, col2 blob,
                                       col1 int primary key auto_increment)
                  engine = InnoDB',
    },
  };
  my $db1 = new_db schema => $schema;
  my $db2 = new_db schema => $schema;
  $db1->execute ('insert into table1 (col2, col3) values (2444, "aabre")');
  $db2->execute ('insert into table1 (col2, col3) values (2444, "aa xy")');
  
  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('master'),
                   default => $db2->source ('master')},
       schema => $schema);

  my $row = $db->select ('table1', ['1 = 1'])->first_as_row;
  is $row->get ('col3'), 'aa xy';

  my $transaction1 = $db1->transaction;
  $db1->execute ('select * from table1 for update');

  my $transaction = $db->transaction;
  dies_ok {
    $row->reload (source_name => 'master', lock => 'update');
  };
  
  $transaction1->rollback;
  $transaction->rollback;

  is $row->get ('col3'), 'aa xy';
} # _reload_in_transaction_locked

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
