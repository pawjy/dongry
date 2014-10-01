package test::Dongry::Database::transaction;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

sub _transaction_committed : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;
  
  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  $db->insert ('foo', [{id => 5431, v1 => "hoge", v2 => undef}]);

  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a,
      [{id => 1243, v1 => "hoge", v2 => undef},
       {id => 5431, v1 => 'hoge', v2 => undef}];
} # _transaction_committed

sub _transaction_reverted : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;
  
  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  $db->insert ('foo', [{id => 5431, v1 => "hoge", v2 => undef}]);

  $transaction->rollback;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [];
} # _transaction_reverted

sub _transaction_committed_nothing : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;

  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [];
} # _transaction_committed_nothing

sub _transaction_committed_twice : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;
  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  $db->insert ('foo', [{id => 5431, v1 => "hoge", v2 => undef}]);

  $transaction->commit;

  dies_here_ok { $transaction->commit };

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a,
      [{id => 1243, v1 => "hoge", v2 => undef},
       {id => 5431, v1 => 'hoge', v2 => undef}];
} # _transaction_committed_twice

sub _transaction_committed_twice_2 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;

  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  $transaction->commit;

  $db->insert ('foo', [{id => 5431, v1 => "hoge", v2 => undef}]);
  dies_here_ok { $transaction->commit };

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a,
      [{id => 1243, v1 => "hoge", v2 => undef},
       {id => 5431, v1 => 'hoge', v2 => undef}];
} # _transaction_committed_twice_2

sub _transaction_committed_rollbacked : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;

  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  $transaction->commit;

  $db->insert ('foo', [{id => 5431, v1 => "hoge", v2 => undef}]);
  dies_here_ok { $transaction->rollback };

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a,
      [{id => 1243, v1 => "hoge", v2 => undef},
       {id => 5431, v1 => 'hoge', v2 => undef}];
} # _transaction_committed_rollbacked

sub _transaction_rollbacked_comitted : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;

  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  $transaction->rollback;

  $db->insert ('foo', [{id => 5431, v1 => "hoge", v2 => undef}]);
  dies_here_ok { $transaction->commit };

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a,
      [{id => 5431, v1 => 'hoge', v2 => undef}];
} # _transaction_rollbacked_comitted

sub _transaction_destroy_rollbacked : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  {
    my $transaction = $db->transaction;
    
    $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  }

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [];
} # _transaction_destroy_rollbacked

sub _transaction_insert_blocked : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int unique key, v1 text) engine=innodb');

  my $time1 = time;

  my $transaction = $db->transaction;
  $db->insert ('foo', [{id => 1243, v1 => "hoge"}]);

  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  dies_here_ok {
    $db2->insert ('foo', [{id => 1243, v1 => "hoge2"}]);
  };

  $transaction->commit;

  my $time_diff = time - $time1;
  ok $time_diff < 2 * 3, "(timeout) $time_diff < 2*3"; # 2s = Test::MySQL::CreateDatabase's default timeout

  my $result = $db->execute
      ('select * from foo order by id asc', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [{id => 1243, v1 => "hoge"}];
} # _transaction_insert_blocked

sub _transaction_select : Test(4) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');

  my $transaction = $db->transaction;
  
  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);
  
  is $db->select ('foo', {id => 1243})->first->{v1}, 'hoge';

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0}});
  is $db2->select ('foo', {id => 1243})->first, undef;
  
  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1243, v1 => 'hoge', v2 => undef}];

  is $db2->select ('foo', {id => 1243})->first->{v1}, 'hoge';
} # _transaction_select

sub _transaction_select_lock_for_update_no_key : Test(5) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text) engine=innodb');
  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);

  my $transaction = $db->transaction;
  $db->select ('foo', {id => 1243}, lock => 'update');
  
  is $db->select ('foo', {id => 1243})->first->{v1}, 'hoge';

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0}});

  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  dies_here_ok {
    $db3->update ('foo', {id => 1245}, where => {id => 1243});
  };

  $db->update ('foo', {id => 1244}, where => {id => 1243});

  is $db2->select ('foo', {id => 1243})->first->{v1}, 'hoge';
  
  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1244, v1 => 'hoge', v2 => undef}];

  is $db2->select ('foo', {id => 1244})->first->{v1}, 'hoge';
} # _transaction_select_lock_for_update_no_key

sub _transaction_select_lock_for_update_with_pkey : Test(6) {
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  $db->execute ('create table foo (id int, v1 text, v2 text,
                 primary key (id) ) engine=innodb');
  $db->insert ('foo', [{id => 1243, v1 => "hoge", v2 => undef}]);

  my $transaction = $db->transaction;
  $db->select ('foo', {id => 1243}, lock => 'update');

  is $db->select ('foo', {id => 1243})->first->{v1}, 'hoge';

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 0}});
  is $db2->select ('foo', {id => 1243})->first->{v1}, 'hoge';

  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  dies_here_ok {
    $db3->update ('foo', {id => 1245}, where => {id => 1243});
  };

  $db->update ('foo', {id => 1244}, where => {id => 1243});
  
  is $db2->select ('foo', {id => 1243})->first->{v1}, 'hoge';

  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1244, v1 => 'hoge', v2 => undef}];

  is $db2->select ('foo', {id => 1244})->first->{v1}, 'hoge';
} # _transaction_select_lock_for_update_with_pkey

sub _transaction_select_lock_count_1 : Test(5) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int, v1 int) engine=innodb');
  $db->insert ('foo', [{id => 1, v1 => 120}]);

  my $transaction = $db->transaction;
  my $count = $db->select ('foo', {id => 1}, lock => 'update')->first->{v1};

  dies_here_ok {
    $db2->select ('foo', {id => 1}, lock => 'update');
  };

  dies_here_ok {
    $db2->select ('foo', {id => 1}, lock => 'share');
  };

  is $db2->select ('foo', {id => 1})->first->{v1}, $count;

  $db->update ('foo', {v1 => $count + 1}, where => {id => 1});
  $transaction->commit;

  my $count2 = $db2->select ('foo', {id => 1}, lock => 'update')->first->{v1};
  is $count2, $count + 1;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1, v1 => 120 + 1}];
} # _transaction_select_lock_count_1

sub _transaction_select_lock_count_2 : Test(5) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int, v1 int) engine=innodb');
  $db->insert ('foo', [{id => 1, v1 => 120}]);

  my $transaction = $db->transaction;
  my $count = $db->select ('foo', {id => 1}, lock => 'share')->first->{v1};

  my $transaction2 = $db2->transaction;
  my $count2 = $db2->select ('foo', {id => 1})->first->{v1};

  my $count3 = $db3->select ('foo', {id => 1})->first->{v1};

  $db->update ('foo', {v1 => $count + 1}, where => {id => 1});

  dies_here_ok {
    $db2->update ('foo', {v1 => $count2 + 10}, where => {id => 1});
  };

  dies_here_ok {
    $db3->update ('foo', {v1 => $count3 + 100}, where => {id => 1});
  };

  $transaction->commit;
  $transaction2->rollback;

  my $count4 = $db2->select ('foo', {id => 1})->first->{v1};
  is $count4, $count + 1;

  my $count5 = $db3->select ('foo', {id => 1})->first->{v1};
  is $count5, $count + 1;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1, v1 => 120 + 1}];
} # _transaction_select_lock_count_2

sub _transaction_select_lock_count_3 : Test(5) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db3 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int, v1 int) engine=innodb');
  $db->insert ('foo', [{id => 1, v1 => 120}]);

  my $transaction = $db->transaction;
  my $count = $db->select ('foo', {id => 1}, lock => 'share')->first->{v1};

  $db->update ('foo', {v1 => $count + 1}, where => {id => 1});

  my $transaction2 = $db2->transaction;
  dies_here_ok {
    $db2->select ('foo', {id => 1}, lock => 'share');
  };

  my $count3 = $db3->select ('foo', {id => 1})->first->{v1};
  is $count3, $count;

  $transaction->commit;
  $transaction2->rollback;

  my $count4 = $db2->select ('foo', {id => 1})->first->{v1};
  is $count4, $count + 1;

  my $count5 = $db3->select ('foo', {id => 1})->first->{v1};
  is $count5, $count + 1;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [{id => 1, v1 => 120 + 1}];
} # _transaction_select_lock_count_3

sub _transaction_select_lock_insert_1 : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int primary key, v1 int) engine=innodb');

  my $transaction = $db->transaction;
  $db->select ('foo', {id => {'<', 10}}, lock => 'update');

  dies_here_ok {
    $db2->insert ('foo', [{id => 5, v1 => 4}]);
  };

  dies_here_ok {
    $db2->insert ('foo', [{id => 16, v1 => 2}]);
  };

  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [];
} # _transaction_select_lock_insert_1

sub _transaction_select_lock_insert_2 : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int primary key, v1 int) engine=innodb');

  my $transaction = $db->transaction;
  $db->select ('foo', {id => 10}, lock => 'update');

  dies_here_ok {
    $db2->insert ('foo', [{id => 10, v1 => 4}]);
  };
  
  dies_here_ok {
    $db2->insert ('foo', [{id => 16, v1 => 2}]);
  };

  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [];
} # _transaction_select_lock_insert_2

sub _transaction_select_lock_insert_3 : Test(3) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int primary key, v1 int) engine=innodb');

  my $transaction = $db->transaction;
  $db->select ('foo', {id => 10}, lock => 'share');

  dies_here_ok {
    $db2->insert ('foo', [{id => 10, v1 => 4}]);
  };
  
  dies_here_ok {
    $db2->insert ('foo', [{id => 16, v1 => 2}]);
  };

  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a, [];
} # _transaction_select_lock_insert_3

sub _transaction_select_lock_insert_4 : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});
  my $db2 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int primary key, v1 int) engine=innodb');
  $db->insert ('foo', [{id => 10}]);

  my $transaction = $db->transaction;
  $db->select ('foo', {id => 10}, lock => 'share');

  dies_here_ok {
    $db2->update ('foo', {id => 10, v1 => 4}, where => {id => 10});
  };
  
  $db2->insert ('foo', [{id => 16, v1 => 2}]);

  $transaction->commit;

  my $result = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result->all->to_a,
      [{id => 10, v1 => undef}, {id => 16, v1 => 2}];
} # _transaction_select_lock_insert_4

sub _transaction_source_name : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}});

  $db->execute ('create table foo (id int primary key, v1 int) engine=innodb');

  my $transaction = $db->transaction;

  dies_here_ok {
    $db->insert ('foo', [{id => 10}], source_name => 'default');
  };

  dies_here_ok {
    $db->select ('foo', {id => 10}, source_name => 'default');
  };

  $transaction->rollback;
} # _transaction_source_name

sub _transaction_source_not_writable : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 0},
                   default => {dsn => $dsn},
                   create => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo (id int primary key, v1 int) engine=innodb',
                undef, source_name => 'create');

  my $transaction = $db->transaction;

  lives_ok { $db->select ('foo', {id => 10}) };

  $transaction->rollback;
} # _transaction_source_not_writable

sub _transaction_in_transaction : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn},
                   create => {dsn => $dsn, writable => 1}});

  $db->execute ('create table foo (id int primary key, v1 int) engine=innodb',
                undef, source_name => 'create');

  my $transaction = $db->transaction;

  dies_here_ok {
    $db->transaction;
  };

  $db->insert ('foo', [{id => 10}]);

  $transaction->commit;

  is $db->select ('foo', {id => 10})->row_count, 1;
} # _transaction_in_transaction

sub _transaction_no_master : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'test1';

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  dies_here_ok {
    my $transaction = $db->transaction;
  };
} # _transaction_no_master

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
