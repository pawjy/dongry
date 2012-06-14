package test::Dongry::Query;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _find_null_query : Test(4) {
  my $db = Dongry::Database->new;
  my $q = $db->query;
  ok $q->is_null;
  is $q->find, undef;
  isa_list_n_ok $q->find_all, 0;
  is $q->count, 0;
} # _find_null_query

sub _find_null_query_filtered : Test(3) {
  my $db = Dongry::Database->new;
  my $q = $db->query (item_list_filter => sub { die });
  is $q->find, undef;
  isa_list_n_ok $q->find_all, 0;
  is $q->count, 0;
} # _find_null_query_filtered

sub _find_no_where : Test(3) {
  my $db = Dongry::Database->new;
  my $q = $db->query (table_name => 'table1');
  dies_here_ok { $q->find };
  dies_here_ok { $q->find_all };
  dies_here_ok { $q->count };
} # _find_no_where

sub _find_not_found : Test(4) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->create ({id => 10});

  my $q = $db->query (table_name => 'table1', where => {id => 1});
  ng $q->is_null;
  is $q->find, undef;
  isa_list_n_ok $q->find_all, 0;
  is $q->count, 0;
} # _find_not_found

sub _find_1_found : Test(6) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->create ({id => 1});

  my $q = $db->query (table_name => 'table1', where => {id => 1});
  my $row = $q->find;
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('id'), 1;
  my $list = $q->find_all;
  isa_list_n_ok $list, 1;
  isa_ok $list->[0], 'Dongry::Table::Row';
  is $list->[0]->get ('id'), 1;
  is $q->count, 1;
} # _find_1_found

sub _find_1_found_zero : Test(6) {
  my $db = new_db schema => {
    0 => {
      _create => 'CREATE TABLE `0` (id INT)',
    },
  };
  $db->table ('0')->create ({id => 1});

  my $q = $db->query (table_name => '0', where => {id => 1});
  my $row = $q->find;
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('id'), 1;
  my $list = $q->find_all;
  isa_list_n_ok $list, 1;
  isa_ok $list->[0], 'Dongry::Table::Row';
  is $list->[0]->get ('id'), 1;
  is $q->count, 1;
} # _find_1_found_zero

sub _find_3_found : Test(7) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 1}, {id => 1}]);

  my $q = $db->query (table_name => 'table1', where => {id => 1});
  my $row = $q->find;
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('id'), 1;
  my $list = $q->find_all;
  isa_list_n_ok $list, 3;
  is $list->[0]->get ('id'), 1;
  is $list->[1]->get ('id'), 1;
  is $list->[2]->get ('id'), 1;
  is $q->count, 3;
} # _find_3_found

sub _find_parsed : Test(7) {
  my $db = new_db schema => {
    table1 => {
      type => {id => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => \1}, {id => \1}, {id => \1}]);

  my $q = $db->query (table_name => 'table1', where => {id => \1});
  my $row = $q->find;
  isa_ok $row, 'Dongry::Table::Row';
  is ${$row->get ('id')}, 1;
  my $list = $q->find_all;
  isa_list_n_ok $list, 3;
  is ${$list->[0]->get ('id')}, 1;
  is ${$list->[1]->get ('id')}, 1;
  is ${$list->[2]->get ('id')}, 1;
  is $q->count, 3;
} # _find_parsed

sub _find_complex : Test(3) {
  my $db = new_db schema => {
    table1 => {
      type => {id => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => \1}, {id => \2}, {id => \3}]);

  my $q = $db->query (table_name => 'table1', where => {id => {-ge => \1}});
  my $row = $q->find;
  isa_ok $row, 'Dongry::Table::Row';
  my $list = $q->find_all;
  isa_list_n_ok $list, 3;
  is $q->count, 3;
} # _find_complex

sub _find_distinct : Test(6) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT, val BLOB)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 1}]);

  my $q = $db->query (table_name => 'table1', where => {id => 1});
  $q->distinct (1);
  $q->fields ('id');
  my $row = $q->find;
  is $row->get ('id'), 1;
  my $list = $q->find_all;
  isa_list_n_ok $list, 1;
  is $list->[0]->get ('id'), 1;
  is $q->count, 1;

  $q->distinct (0);
  isa_list_n_ok $q->find_all, 2;
  is $q->count, 2;
} # _find_distinct

sub _find_fields : Test(4) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT, val BLOB)',
    },
  };
  $db->table ('table1')->insert ([{id => 1, val => 10}]);

  my $q = $db->query (table_name => 'table1', where => {id => 1});
  $q->fields ($db->bare_sql_fragment ('val * 2 AS value'));
  my $row = $q->find;
  is $row->get ('value'), 20;
  my $list = $q->find_all;
  isa_list_n_ok $list, 1;
  is $list->[0]->get ('value'), 20;
  is $q->count, 1;
} # _find_fields

sub _find_group : Test(8) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT, val BLOB)',
    },
  };
  $db->table ('table1')->insert
      ([{id => 1, val => 10}, {id => 2, val => 13}, {id => 1, val => 20}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       fields => ['id', {-sum => 'val', as => 'value'}],
       group => ['id'],
       order => [id => 1]);
  my $row = $q->find;
  is $row->get ('id'), 1;
  is $row->get ('value'), 30;
  my $list = $q->find_all;
  isa_list_n_ok $list, 2;
  is $list->[0]->get ('id'), 1;
  is $list->[0]->get ('value'), 30;
  is $list->[1]->get ('id'), 2;
  is $list->[1]->get ('value'), 13;
  is $q->count, 2;
} # _find_group

sub _find_group_not_found : Test(3) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT, val BLOB)',
    },
  };
  $db->table ('table1')->insert
      ([{id => 1, val => 10}, {id => 2, val => 13}, {id => 1, val => 20}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 10}},
       fields => ['id', {-sum => 'val', as => 'value'}],
       group => ['id'],
       order => [id => 1]);
  my $row = $q->find;
  is $row, undef;
  my $list = $q->find_all;
  isa_list_n_ok $list, 0;
  is $q->count, 0;
} # _find_group_not_found

sub _find_group_multiple : Test(12) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT, val BLOB)',
    },
  };
  $db->table ('table1')->insert
      ([{id => 1, val => 10}, {id => 2, val => 13}, {id => 1, val => 20},
        {id => 3, val => 10}, {id => 2, val => 13}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       fields => ['id', {-sum => 'val', as => 'value'}],
       group => ['id', 'val'],
       order => [id => 1, value => 1]);
  my $row = $q->find;
  is $row->get ('id'), 1;
  is $row->get ('value'), 10;
  my $list = $q->find_all;
  isa_list_n_ok $list, 4;
  is $list->[0]->get ('id'), 1;
  is $list->[0]->get ('value'), 10;
  is $list->[1]->get ('id'), 1;
  is $list->[1]->get ('value'), 20;
  is $list->[2]->get ('id'), 2;
  is $list->[2]->get ('value'), 26;
  is $list->[3]->get ('id'), 3;
  is $list->[3]->get ('value'), 10;
  is $q->count, 4;
} # _find_group_multiple

sub _find_order : Test(7) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT, v BLOB)',
    },
  };
  $db->table ('table1')->insert
      ([{id => 1, v => 1}, {id => 1, v => 2}, {id => 1, v => 3}]);

  my $q = $db->query
      (table_name => 'table1', where => {id => 1}, order => [v => 1]);
  my $row = $q->find;
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('v'), 1;
  my $list = $q->find_all;
  isa_list_n_ok $list, 3;
  is $list->[0]->get ('v'), 1;
  is $list->[1]->get ('v'), 2;
  is $list->[2]->get ('v'), 3;
  is $q->count, 3;
} # _find_order

sub _find_order_2 : Test(7) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT, v BLOB)',
    },
  };
  $db->table ('table1')->insert
      ([{id => 1, v => 1}, {id => 1, v => 2}, {id => 1, v => 3}]);

  my $q = $db->query
      (table_name => 'table1', where => {id => 1}, order => [v => -1]);
  my $row = $q->find;
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('v'), 3;
  my $list = $q->find_all;
  isa_list_n_ok $list, 3;
  is $list->[0]->get ('v'), 3;
  is $list->[1]->get ('v'), 2;
  is $list->[2]->get ('v'), 1;
  is $q->count, 3;
} # _find_order

sub _find_3_offset : Test(5) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1', where => {id => {-not => undef}},
       order => [id => 1]);
  my $row = $q->find (offset => 1);
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('id'), 1;
  my $list = $q->find_all (offset => 1);
  isa_list_n_ok $list, 1;
  is $list->[0]->get ('id'), 2;
  is $q->count (offset => 1), 3;
} # _find_3_offset

sub _find_3_limit : Test(6) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1', where => {id => {-not => undef}},
       order => [id => 1]);
  my $row = $q->find (limit => 2);
  isa_ok $row, 'Dongry::Table::Row';
  is $row->get ('id'), 1;
  my $list = $q->find_all (limit => 2);
  isa_list_n_ok $list, 2;
  is $list->[0]->get ('id'), 1;
  is $list->[1]->get ('id'), 2;
  is $q->count (limit => 2), 3;
} # _find_3_limit

sub _find_and_where : Test(3) {
  my $db = new_db schema => {foo => {}};
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2), (3)');
  
  my $q = $db->query (table_name => 'foo',
                      where => {id => {-gt => 0}},
                      order => [id => -1]);

  my $row = $q->find (and_where => {id => {-lt => 3}});
  is $row->get ('id'), 2;

  my $list = $q->find_all (and_where => {id => {-lt => 3}});
  eq_or_diff $list->map (sub { $_->get ('id') })->to_a, [2, 1];

  is $q->count (and_where => {id => {-lt => 3}}), 2;
} # _find_and_where

sub _find_and_where_undef : Test(3) {
  my $db = new_db schema => {foo => {}};
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (1), (2), (3)');
  
  my $q = $db->query (table_name => 'foo',
                      where => {id => {-gt => 0}},
                      order => [id => -1]);

  my $row = $q->find (and_where => undef);
  is $row->get ('id'), 3;

  my $list = $q->find_all (and_where => undef);
  eq_or_diff $list->map (sub { $_->get ('id') })->to_a, [3, 2, 1];

  is $q->count (and_where => undef), 3;
} # _find_and_where_undef

sub _find_source_name : Test(22) {
  my $db1 = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  my $db2 = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db1->table ('table1')->insert ([{id => 1}]);
  $db2->table ('table1')->insert ([{id => 2}, {id => 2}]);

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('default'),
                   default => $db2->source ('default')},
       schema => {table1 => {}});

  my $q = $db->query
      (table_name => 'table1', where => {id => {-not => undef}});

  {
    my $row = $q->find (source_name => 'master');
    isa_ok $row, 'Dongry::Table::Row';
    is $row->get ('id'), 1;
    my $list = $q->find_all (source_name => 'master');
    isa_list_n_ok $list, 1;
    is $list->[0]->get ('id'), 1;
    is $q->count (source_name => 'master'), 1;
  }
  {
    my $row = $q->find (source_name => 'default');
    isa_ok $row, 'Dongry::Table::Row';
    is $row->get ('id'), 2;
    my $list = $q->find_all (source_name => 'default');
    isa_list_n_ok $list, 2;
    is $list->[0]->get ('id'), 2;
    is $list->[1]->get ('id'), 2;
    is $q->count (source_name => 'default'), 2;
  }
  {
    my $row = $q->find;
    isa_ok $row, 'Dongry::Table::Row';
    is $row->get ('id'), 2;
    my $list = $q->find_all;
    isa_list_n_ok $list, 2;
    is $list->[0]->get ('id'), 2;
    is $list->[1]->get ('id'), 2;
    is $q->count, 2;
  }

  {
    $q->source_name ('master');
    my $row = $q->find;
    isa_ok $row, 'Dongry::Table::Row';
    is $row->get ('id'), 1;
    my $list = $q->find_all;
    isa_list_n_ok $list, 1;
    is $list->[0]->get ('id'), 1;
    is $q->count, 1;
  }
} # _find_source_name

sub _find_filtered : Test(8) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 1}, {id => 1}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => 1},
       item_list_filter => sub {
         return $_[1]->map (sub { $_->{data}->{id}++; [$_] });
       });

  my $row = $q->find;
  is scalar @$row, 1;
  isa_ok $row->[0], 'Dongry::Table::Row';
  is $row->[0]->get ('id'), 2;
  my $list = $q->find_all;
  isa_list_n_ok $list, 3;
  is $list->[0]->[0]->get ('id'), 2;
  is $list->[1]->[0]->get ('id'), 2;
  is $list->[2]->[0]->get ('id'), 2;
  is $q->count, 3;
} # _find_filtered

sub _find_null_cb : Test(6) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  my $invoked;
  my $result;
  my $value;
  $q->find (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });
  
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $value, undef;
} # _find_null_cb

sub _find_all_null_cb : Test(6) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  my $invoked;
  my $result;
  my $value;
  $q->find_all (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });
  
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  isa_list_n_ok $value, 0;
} # _find_all_null_cb

sub _count_null_cb : Test(6) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  my $invoked;
  my $result;
  my $value;
  $q->count (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });
  
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $value, 0;
} # _count_null_cb

sub _find_null_cb_return : Test(7) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  my $invoked;
  my $result;
  my $value;
  my $return = $q->find (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });
  
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $value, undef;
  is $return, $value;
} # _find_cb_return

sub _find_all_null_cb_return : Test(7) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  my $invoked;
  my $result;
  my $value;
  my $return = $q->find_all (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });
  
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  isa_list_n_ok $value, 0;
  is $return, $value;
} # _find_all_cb_return

sub _count_null_cb_return : Test(7) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  my $invoked;
  my $result;
  my $value;
  my $return = $q->count (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
  });
  
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $value, 0;
  is $return, $value;
} # _count_cb_return

sub _find_null_cb_exception : Test(1) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  eval {
    $q->find (cb => sub {
      die "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 4]}\E\.?\n$};
} # _find_null_cb_exception

sub _find_all_null_cb_exception : Test(1) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  eval {
    $q->find_all (cb => sub {
      die "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 4]}\E\.?\n$};
} # _find_all_null_cb_exception

sub _count_null_cb_exception : Test(1) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  eval {
    $q->count (cb => sub {
      die "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 4]}\E\.?\n$};
} # _count_null_cb_exception

sub _find_null_cb_exception_carp : Test(1) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  eval {
    $q->find (cb => sub {
      Carp::croak "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 3]}\E\.?\n$};
} # _find_null_cb_exception_carp

sub _find_all_null_cb_exception_carp : Test(1) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  eval {
    $q->find_all (cb => sub {
      Carp::croak "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 3]}\E\.?\n$};
} # _find_all_null_cb_exception_carp

sub _count_null_cb_exception_carp : Test(1) {
  my $db = Dongry::Database->new;
  my $q = $db->query;

  eval {
    $q->count (cb => sub {
      Carp::croak "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 3]}\E\.?\n$};
} # _count_null_cb_exception_carp

sub _find_filtered_cb : Test(7) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  $q->find (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, [2];
} # _find_filtered_cb

sub _find_all_filtered_cb : Test(8) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  $q->find_all (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  isa_list_n_ok $value, 3;
  eq_or_diff $value->to_a, [[2], [3], [4]];
} # _find_all_filtered_cb

sub _count_filtered_cb : Test(7) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  $q->count (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, 3;
} # _count_filtered_cb

sub _count_filtered_cb_zero : Test(7) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  $q->count (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, 0;
} # _count_filtered_cb

sub _find_filtered_cb_return : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  my $return = $q->find (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, [2];
  is $return, $value;
} # _find_filtered_cb_return

sub _find_all_filtered_cb_return : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  my $return = $q->find_all (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  isa_list_n_ok $value, 3;
  eq_or_diff $value->to_a, [[2], [3], [4]];
  is $return, $value;
} # _find_all_filtered_cb_return

sub _count_filtered_cb_return : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  my $return = $q->count (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, 3;
  is $return, $value;
} # _count_filtered_cb_return

sub _count_filtered_cb_return_zero : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $result;
  my $value;
  my $invoked;
  my $return = $q->count (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, 0;
  is $return, $value;
} # _count_filtered_cb_return_zero

sub _find_filtered_cb_error : Test(2) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $invoked;
  dies_here_ok {
    $q->find (cb => sub {
      $invoked++;
    });
  };
  ng $invoked;
} # _find_filtered_cb_error

sub _find_all_filtered_cb_error : Test(2) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $invoked;
  dies_here_ok {
    $q->find_all (cb => sub {
      $invoked++;
    });
  };
  ng $invoked;
} # _find_all_filtered_cb_error

sub _count_filtered_cb_error : Test(2) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $invoked;
  dies_here_ok {
    $q->count (cb => sub {
      $invoked++;
    });
  };
  ng $invoked;
} # _count_filtered_cb_error

sub _find_filtered_cb_exception : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  eval {
    $q->find (cb => sub {
      die "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 4]}\E\.?\n$};
} # _find_filtered_cb_exception

sub _find_all_filtered_cb_exception : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  eval {
    $q->find_all (cb => sub {
      die "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 4]}\E\.?\n$};
} # _find_all_filtered_cb_exception

sub _count_filtered_cb_exception : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  eval {
    $q->count (cb => sub {
      die "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 4]}\E\.?\n$};
} # _count_filtered_cb_exception

sub _find_filtered_cb_exception_carp : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  eval {
    $q->find (cb => sub {
      Carp::croak "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 3]}\E\.?\n$};
} # _find_filtered_cb_exception_carp

sub _find_all_filtered_cb_exception_carp : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  eval {
    $q->find_all (cb => sub {
      Carp::croak "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 3]}\E\.?\n$};
} # _find_all_filtered_cb_exception_carp

sub _count_filtered_cb_exception_carp : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  };
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  eval {
    $q->count (cb => sub {
      Carp::croak "abc";
    });
    ng 1;
  };
  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 3]}\E\.?\n$};
} # _count_filtered_cb_exception_carp

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
