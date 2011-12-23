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

# ------ |find| and |find_all| ------

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

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
