package test::Dongry::Table::find;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Type::DateTime;
use Encode;

# ------ |find| and |find_all| ------

sub _find_no_source : Test(1) {
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

  dies_here_ok {
    $db->table ('table1')->find
        ({}, source_name => 'notfound');
  };
} # _find_no_source

sub _find_parsable_1 : Test(7) {
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

  my $row = $db->table ('table1')->find
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"});
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2001-04-01T00:12:50';
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"});
  isa_list_n_ok $row_list, 1;
  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is_datetime $row_list->[0]->get ('col1'), '2001-04-01T00:12:50';
  is ${$row_list->[0]->get ('col2')}, 'abc def';
} # _find_parsable_1

sub _find_parsable_many : Test(13) {
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
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');

  my $row = $db->table ('table1')->find
      ({col2 => \"abc def"},
       order => [col1 => 1]);
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2001-04-01T00:12:50';
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => \"abc def"}, order => [col1 => 1]);
  isa_list_n_ok $row_list, 3;

  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is_datetime $row_list->[0]->get ('col1'), '2001-04-01T00:12:50';
  is ${$row_list->[0]->get ('col2')}, 'abc def';

  isa_ok $row_list->[1], 'Dongry::Table::Row';
  is_datetime $row_list->[1]->get ('col1'), '2001-05-01T00:12:50';
  is ${$row_list->[1]->get ('col2')}, 'abc def';

  isa_ok $row_list->[2], 'Dongry::Table::Row';
  is_datetime $row_list->[2]->get ('col1'), '2002-04-01T00:12:50';
  is ${$row_list->[2]->get ('col2')}, 'abc def';
} # _find_parsable_many

sub _find_parsable_none : Test(2) {
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
                 values ("2001-04-01 00:12:50", "abc defg")');

  my $row = $db->table ('table1')->find
      ({col2 => \"abc def"},
       order => [col1 => 1]);
  is $row, undef;

  my $row_list = $db->table ('table1')->find_all
      ({col2 => \"abc def"}, order => [col1 => 1]);
  isa_list_n_ok $row_list, 0;
} # _find_parsable_none

sub _find_non_parsable_ref : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  dies_here_ok {
    my $row = $db->table ('table1')->find
        ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                                 hour => 0, minute => 12, second => 50),
          col2 => \"abc def"});
  };

  dies_here_ok {
    my $row_list = $db->table ('table1')->find_all
        ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                                 hour => 0, minute => 12, second => 50),
          col2 => \"abc def"});
  };
} # _find_non_parsable_ref

sub _find_unknown_type : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_unknown',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  dies_here_ok {
    my $row = $db->table ('table1')->find
        ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                                 hour => 0, minute => 12, second => 50),
          col2 => \"abc def"});
  };

  dies_here_ok {
    my $row_list = $db->table ('table1')->find_all
        ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                                 hour => 0, minute => 12, second => 50),
          col2 => \"abc def"});
  };
} # _find_unknown_type

sub _find_no_schema : Test(2) {
  my $schema = {
  };
  my $db = new_db schema => $schema;

  dies_here_ok {
    my $row = $db->table ('table1')->find
        ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                                 hour => 0, minute => 12, second => 50),
          col2 => \"abc def"});
  };

  dies_here_ok {
    my $row_list = $db->table ('table1')->find_all
        ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                                 hour => 0, minute => 12, second => 50),
          col2 => \"abc def"});
  };
} # _find_no_schema

sub _find_not_parsable : Test(7) {
  my $schema = {
    table1 => {
      type => {},
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->table ('table1')->find
      ({col1 => '2001-04-01 00:12:50',
        col2 => "abc def"});
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('col1'), '2001-04-01 00:12:50';
  is $row->get ('col2'), 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col1 => '2001-04-01 00:12:50',
        col2 => "abc def"});
  isa_list_n_ok $row_list, 1;
  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is $row_list->[0]->get ('col1'), '2001-04-01 00:12:50';
  is $row_list->[0]->get ('col2'), 'abc def';
} # _find_not_parsable

sub _find_fields : Test(7) {
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

  my $row = $db->table ('table1')->find
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"},
       fields => 'col1');
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2001-04-01T00:12:50';
  dies_here_ok { $row->get ('col2') };

  my $row_list = $db->table ('table1')->find_all
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"},
       fields => 'col2');
  isa_list_n_ok $row_list, 1;
  isa_ok $row_list->[0], 'Dongry::Table::Row';
  dies_here_ok { $row_list->[0]->get ('col1') };
  is ${$row_list->[0]->get ('col2')}, 'abc def';
} # _find_fields

sub _find_fields_distinct : Test(7) {
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
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->table ('table1')->find
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"},
       fields => 'col1');
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2001-04-01T00:12:50';
  dies_here_ok { $row->get ('col2') };

  my $row_list = $db->table ('table1')->find_all
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"},
       fields => 'col2',
       distinct => 1);
  isa_list_n_ok $row_list, 1;
  isa_ok $row_list->[0], 'Dongry::Table::Row';
  dies_here_ok { $row_list->[0]->get ('col1') };
  is ${$row_list->[0]->get ('col2')}, 'abc def';
} # _find_fields_distinct

sub _find_group : Test(10) {
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
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-02 00:12:50", "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-03 00:12:50", "abc xyz")');

  my $row = $db->table ('table1')->find
      ({col2 => {-not => undef}},
       fields => [{-max => 'col1', as => 'col2'},
                  {-count => undef, as => 'col3'}],
       group => ['col2'],
       order => [col3 => -1]);
  isa_ok $row, 'Dongry::Table::Row';
  is ${$row->get ('col2')}, '2001-04-02 00:12:50';
  is $row->get ('col3'), 2;

  my $row_list = $db->table ('table1')->find_all
      ({col2 => {-not => undef}},
       fields => [{-max => 'col1', as => 'col2'},
                  {-count => undef, as => 'col3'}],
       group => ['col2'],
       order => [col3 => -1]);
  isa_list_n_ok $row_list, 2;

  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is ${$row_list->[0]->get ('col2')}, '2001-04-02 00:12:50';
  is $row_list->[0]->get ('col3'), 2;

  isa_ok $row_list->[1], 'Dongry::Table::Row';
  is ${$row_list->[1]->get ('col2')}, '2001-04-03 00:12:50';
  is $row_list->[1]->get ('col3'), 1;
} # _find_group

sub _find_order : Test(13) {
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
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');

  my $row = $db->table ('table1')->find
      ({col2 => \"abc def"},
       order => [col1 => -1]);
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2002-04-01T00:12:50';
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => \"abc def"}, order => [col1 => -1]);
  isa_list_n_ok $row_list, 3;

  isa_ok $row_list->[2], 'Dongry::Table::Row';
  is_datetime $row_list->[2]->get ('col1'), '2001-04-01T00:12:50';
  is ${$row_list->[2]->get ('col2')}, 'abc def';

  isa_ok $row_list->[1], 'Dongry::Table::Row';
  is_datetime $row_list->[1]->get ('col1'), '2001-05-01T00:12:50';
  is ${$row_list->[1]->get ('col2')}, 'abc def';

  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is_datetime $row_list->[0]->get ('col1'), '2002-04-01T00:12:50';
  is ${$row_list->[0]->get ('col2')}, 'abc def';
} # _find_order

sub _find_offset : Test(7) {
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
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');

  my $row = $db->table ('table1')->find
      ({col2 => \"abc def"},
       order => [col1 => -1],
       offset => 1);
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2001-05-01T00:12:50';
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => \"abc def"}, order => [col1 => -1], offset => 1);
  isa_list_n_ok $row_list, 1;

  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is_datetime $row_list->[0]->get ('col1'), '2001-05-01T00:12:50';
  is ${$row_list->[0]->get ('col2')}, 'abc def';
} # _find_offset

sub _find_limit : Test(10) {
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
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');

  my $row = $db->table ('table1')->find
      ({col2 => \"abc def"},
       order => [col1 => -1],
       limit => 2);
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2002-04-01T00:12:50';
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => \"abc def"}, order => [col1 => -1], limit => 2);
  isa_list_n_ok $row_list, 2;

  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is_datetime $row_list->[0]->get ('col1'), '2002-04-01T00:12:50';
  is ${$row_list->[0]->get ('col2')}, 'abc def';

  isa_ok $row_list->[1], 'Dongry::Table::Row';
  is_datetime $row_list->[1]->get ('col1'), '2001-05-01T00:12:50';
  is ${$row_list->[1]->get ('col2')}, 'abc def';
} # _find_limit

sub _find_and_where : Test(2) {
  my $db = new_db schema => {foo => {}};
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2), (3)');
  
  my $table = $db->table ('foo');

  my $row = $table->find ({id => {-gt => 0}},
                          and_where => {id => {-lt => 3}},
                          order => [id => -1]);
  is $row->get ('id'), 2;

  my $list = $table->find_all ({id => {-gt => 0}},
                               and_where => {id => {-lt => 3}},
                               order => [id => -1]);
  eq_or_diff $list->map (sub { $_->get ('id') })->to_a, [2, 1];
} # _find_and_where

sub _find_and_where_undef : Test(2) {
  my $db = new_db schema => {foo => {}};
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2), (3)');
  
  my $table = $db->table ('foo');

  my $row = $table->find ({id => {-gt => 0}},
                          and_where => undef,
                          order => [id => -1]);
  is $row->get ('id'), 3;

  my $list = $table->find_all ({id => {-gt => 0}},
                               and_where => undef,
                               order => [id => -1]);
  eq_or_diff $list->map (sub { $_->get ('id') })->to_a, [3, 2, 1];
} # _find_and_where_undef

sub _find_source_name : Test(9) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db1 = new_db schema => $schema;
  my $db2 = new_db schema => $schema;
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "xyz def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "xyz def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "xyz def")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $db1->source ('default')->{dsn}},
                   default => {dsn => $db2->source ('default')->{dsn}}},
       schema => $schema);

  my $row = $db->table ('table1')->find
      ({col2 => {-not => undef}},
       order => [col1 => -1],
       source_name => 'master');
  is ${$row->get ('col2')}, 'abc def';

  my $row2 = $db->table ('table1')->find
      ({col2 => {-not => undef}},
       order => [col1 => -1],
       source_name => 'default');
  is ${$row2->get ('col2')}, 'xyz def';

  my $row3 = $db->table ('table1')->find
      ({col2 => {-not => undef}},
       order => [col1 => -1]);
  is ${$row3->get ('col2')}, 'xyz def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => {-not => undef}}, order => [col1 => -1],
       source_name => 'master');
  isa_list_n_ok $row_list, 3;
  is ${$row_list->[0]->get ('col2')}, 'abc def';

  my $row_list2 = $db->table ('table1')->find_all
      ({col2 => {-not => undef}}, order => [col1 => -1],
       source_name => 'default');
  isa_list_n_ok $row_list2, 3;
  is ${$row_list2->[0]->get ('col2')}, 'xyz def';

  my $row_list3 = $db->table ('table1')->find_all
      ({col2 => {-not => undef}}, order => [col1 => -1]);
  isa_list_n_ok $row_list3, 3;
  is ${$row_list3->[0]->get ('col2')}, 'xyz def';
} # _find_source_name

sub _find_transaction : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => ['col1'],
      _create => 'create table table1 (col1 timestamp, col2 blob)
                  engine = InnoDB',
    },
  };
  my $db1 = new_db schema => $schema;
  my $db2 = new_db schema => $schema;
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "xyz def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "xyz def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "xyz def")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $db1->source ('default')->{dsn},
                              writable => 1},
                   default => {dsn => $db2->source ('default')->{dsn}}},
       schema => $schema);

  my $transaction = $db->transaction;

  my $row = $db->table ('table1')->find
      ({col2 => {-not => undef}},
       order => [col1 => -1]);
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => {-not => undef}}, order => [col1 => -1]);
  isa_list_n_ok $row_list, 3;
  is ${$row_list->[0]->get ('col2')}, 'abc def';

  $db->table ('table1')->find
      ({col2 => {-not => undef}},
       lock => 'update');

  my $transaction1 = $db1->transaction;

  dies_here_ok {
    $db1->table ('table1')->find
        ({col2 => {-not => undef}},
         lock => 'update');
  };

  $transaction1->rollback;
  $transaction->rollback;
} # _find_transaction

sub _find_transaction_2 : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => ['col1'],
      _create => 'create table table1 (col1 timestamp, col2 blob)
                  engine = InnoDB',
    },
  };
  my $db1 = new_db schema => $schema;
  my $db2 = new_db schema => $schema;
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db1->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "xyz def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "xyz def")');
  $db2->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "xyz def")');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $db1->source ('default')->{dsn},
                              writable => 1},
                   default => {dsn => $db2->source ('default')->{dsn}}},
       schema => $schema);

  my $transaction = $db->transaction;

  my $row = $db->table ('table1')->find
      ({col2 => {-not => undef}},
       order => [col1 => -1]);
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => {-not => undef}}, order => [col1 => -1]);
  isa_list_n_ok $row_list, 3;
  is ${$row_list->[0]->get ('col2')}, 'abc def';

  $db->table ('table1')->find_all
      ({col2 => {-not => undef}},
       lock => 'update');

  my $transaction1 = $db1->transaction;

  dies_here_ok {
    $db1->table ('table1')->find_all
        ({col2 => {-not => undef}},
         lock => 'update');
  };

  $transaction1->rollback;
  $transaction->rollback;
} # _find_transaction_2

sub _find_cb : Test(9) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  };
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $result;
  my $value;
  $db->table ('foo')->find ({id => {-gt => 4}},
                            order => [id => 1], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };
  isa_ok $value, 'Dongry::Table::Row';
  is $value->table_name, 'foo';
  eq_or_diff $value->{data}, {id => 12, value => '2012-01-01 00:12:12'};
} # _find_cb

sub _find_cb_not_found : Test(7) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  };
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $result;
  my $value;
  $db->table ('foo')->find ({id => {-gt => 400}},
                            order => [id => 1], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };
  is $value, undef;
} # _find_cb_not_found

sub _find_all_cb : Test(13) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  };
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $result;
  my $value;
  $db->table ('foo')->find_all ({id => {-gt => 4}},
                                order => [id => 1], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };
  isa_list_n_ok $value, 2;
  isa_ok $value->[0], 'Dongry::Table::Row';
  is $value->[0]->table_name, 'foo';
  eq_or_diff $value->[0]->{data}, {id => 12, value => '2012-01-01 00:12:12'};
  isa_ok $value->[1], 'Dongry::Table::Row';
  is $value->[1]->table_name, 'foo';
  eq_or_diff $value->[1]->{data}, {id => 21, value => '1991-02-12 12:12:01'};
} # _find_all_cb

sub _find_cb_return : Test(10) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  };
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $result;
  my $value;
  my $return = $db->table ('foo')->find ({id => {-gt => 4}},
                                         order => [id => 1], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };

  isa_ok $return, 'Dongry::Table::Row';
  is $return->table_name, 'foo';
  eq_or_diff $return->{data}, {id => 12, value => '2012-01-01 00:12:12'};
  is $value, $return;
} # _find_cb_return

sub _find_cb_return_not_found : Test(8) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  };
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $result;
  my $value;
  my $return = $db->table ('foo')->find ({id => {-gt => 400}},
                                         order => [id => 1], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };

  is $return, undef;
  is $value, undef;
} # _find_cb_return_not_found

sub _find_all_cb_return : Test(14) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  };
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $result;
  my $value;
  my $return = $db->table ('foo')->find_all ({id => {-gt => 4}},
                                             order => [id => 1], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $result->table_name, 'foo';
  dies_here_ok { $result->all };

  isa_list_n_ok $return, 2;
  isa_ok $return->[0], 'Dongry::Table::Row';
  is $return->[0]->table_name, 'foo';
  eq_or_diff $return->[0]->{data}, {id => 12, value => '2012-01-01 00:12:12'};
  isa_ok $return->[1], 'Dongry::Table::Row';
  is $return->[1]->table_name, 'foo';
  eq_or_diff $return->[1]->{data}, {id => 21, value => '1991-02-12 12:12:01'};
  is $value, $return;
} # _find_all_cb_return

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
