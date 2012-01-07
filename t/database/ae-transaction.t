package test::Dongry::Database::anyevent::transaction;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

sub _execute_cb_all : Test(8) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');

  my $success;
  my $error;
  my $result;
  my $transaction = $db->transaction;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->commit (cb => sub {
      if ($_[1]->is_success) {
        $success++;
        is $_[0], $db;
      }
      if ($_[1]->is_error) {
        $transaction->rollback (cb => sub {
          $error++;
          $cv->send;
        });
      }
      $result = $_[1];
      $cv->send;
    });
  });
  
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;

  is $success, 1;
  my $result2 = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result2->all->to_a, [{id => 1}, {id => 2}];
} # _execute_cb_all

sub _execute_commit_failed : Test(7) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  require AnyEvent::DBI;
  no warnings 'redefine';
  local *AnyEvent::DBI::req_commit = sub {
    die ["commit error"];
  };

  $db->execute ('create table foo (id int) engine=innodb');

  my $success;
  my $error;
  my $result;
  my $transaction = $db->transaction;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->commit (cb => sub {
      if ($_[1]->is_success) {
        $error++;
      }
      if ($_[1]->is_error) {
        $success++;
      }
      $result = $_[1];
      $cv->send;
    });
  });
  
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  is $result->error_text, 'commit error';
  is $result->error_sql, 'commit';

  is $success, 1;
  my $result2 = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result2->all->to_a, [];
} # _execute_commit_failed

sub _execute_cb_rollback : Test(8) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');

  my $success;
  my $error;
  my $result;
  my $transaction = $db->transaction;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->rollback (cb => sub {
      if ($_[1]->is_success) {
        $success++;
        is $_[0], $db;
      } else {
        $transaction->commit (cb => sub {
          $error++;
          $cv->send;
        });
      }
      $result = $_[1];
      $cv->send;
    });
  });
  
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;

  is $success, 1;
  my $result2 = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result2->all->to_a, [];
} # _execute_cb_rollback

sub _execute_cb_rollback_failed : Test(9) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;
  my $dsn = $db->source ('master')->{dsn};

  my $cv = AnyEvent->condvar;
  
  require AnyEvent::DBI;
  no warnings 'redefine';
  local *AnyEvent::DBI::req_rollback = sub {
    die ["rollback error"];
  };
  
  $db->execute ('create table foo (id int) engine=innodb');
  
  my $success;
  my $error;
  my $result;
  my $transaction = $db->transaction;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->rollback (cb => sub {
      if ($_[1]->is_success) {
        $error++;
      } else {
        $success++;
        is $_[0], $db;
        is $@, 'rollback error';
      }
      $result = $_[1];
      $cv->send;
    });
  });
    
  $cv->recv;
  undef $transaction;
  undef $db;
  ## Implicit rollback error.

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  is $result->error_text, 'rollback error';
  is $result->error_sql, 'rollback';

  is $success, 1;
  $db = Dongry::Database->new (sources => {default => {dsn => $dsn}});
  my $result2 = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result2->all->to_a, [];
} # _execute_cb_rollback_failed

sub _execute_cb_transaction_destroyed_too_early : Test(3) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;
  my $dsn = $db->source ('master')->{dsn};

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');

  my $error;
  my $transaction = $db->transaction;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->commit (cb => sub {
      $error++;
      $cv->send;
    });
  });
  undef $transaction;
  
  dies_ok {
    $cv->recv;
  };
  undef $db;
  undef $transaction;

  ng $error;
  $db = Dongry::Database->new (sources => {default => {dsn => $dsn}});
  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
} # _execute_cb_transaction_destroyed_too_early

sub _execute_cb_transaction_destroyed_before_commit : Test(3) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;
  my $dsn = $db->source ('master')->{dsn};

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');

  my $success;
  my $error;
  my $transaction = $db->transaction;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->commit (cb => sub {
      $_[1]->is_success ? $success++ : $error++;
      $cv->send;
    });
    undef $transaction;
  });
  
  $cv->recv;
  undef $db;
  undef $transaction;

  is $success, 1;
  ng $error;
  $db = Dongry::Database->new (sources => {default => {dsn => $dsn}});
  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [{id => 1}, {id => 2}];
} # _execute_cb_transaction_destroyed_before_commit

sub _execute_after_transaction : Test(3) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');
  $cv->begin;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $cv->end;
  });

  $cv->begin;
  $transaction->rollback (cb => sub {
    $cv->end;
  });

  $cv->begin;
  $db->execute ('insert into foo (id) values (3)', undef, cb => sub {
    $cv->end;
  });
  
  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [{id => 3}];
} # _execute_after_transaction

sub _execute_after_in_transaction : Test(3) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');
  $cv->begin;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->rollback (cb => sub {
      $cv->end;
    });
  });
  
  $cv->begin;
  $db->execute ('insert into foo (id) values (3)', undef, cb => sub {
    $cv->end;
  });
  
  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
} # _execute_after_in_transaction

sub _execute_after_transaction_failed : Test(3) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  require AnyEvent::DBI;
  no warnings 'redefine';
  local *AnyEvent::DBI::req_rollback = sub {
    die ["rollback error"];
  };

  $db->execute ('create table foo (id int) engine=innodb');
  $cv->begin;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $cv->end;
  });

  $cv->begin;
  $transaction->rollback (cb => sub {
    $cv->end;
  });

  $cv->begin;
  $db->execute ('insert into foo (id) values (3)', undef, cb => sub {
    $cv->end;
  });

  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
} # _execute_after_transaction_failed

sub _execute_transaction_execute_failed : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');
  $cv->begin;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $cv->end;
  });

  my $called;
  $cv->begin;
  $db->execute ('insert syntax error', undef, cb => sub {
    $called++ if $_[1]->is_error;
    $cv->end;
  });

  $cv->begin;
  $db->execute ('insert into foo (id) values (3), (4)', undef, cb => sub {
    $cv->end;
  });

  $cv->begin;
  $transaction->commit (cb => sub {
    $cv->end;
  });

  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
  is $called, 1;
} # _execute_after_transaction_failed

sub _execute_transaction_nested : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int) engine=innodb');
  $cv->begin;

  my $called;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $transaction->commit (cb => sub {
      $called++ if $_[1]->is_success;
      $cv->end;
    });
    undef $transaction;
  }, onerror => sub {
    $cv->end;
  });

  $cv->end;
  $cv->recv;

  is $called, 1;
  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [{id => 1}, {id => 2}];
} # _execute_transaction_nested

sub _execute_transaction_nested_error : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int primary key) engine=innodb');
  $cv->begin;

  my $called;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $db->execute ('insert into foo (id) values (2), (3)', undef, cb => sub {
      if ($_[1]->is_success) {
        $transaction->commit (cb => sub {
          $cv->end;
        });
        undef $transaction;
      } else {
        $called++;
        $transaction->rollback (cb => sub {
          $called++ if $_[1]->is_error;
          $cv->end;
        });
        undef $transaction;
      }
    }) if $_[1]->is_success;
    $cv->end if $_[1]->is_error;
  });

  $cv->end;
  $cv->recv;

  is $called, 2;
  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
} # _execute_transaction_nested_error

sub _execute_transaction_nested_error_2 : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int primary key) engine=innodb');
  $cv->begin;

  my $called;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $db->execute ('insert into foo (id) values (2), (3)', undef, cb => sub {
      $called++ if $_[1]->is_error;
      $transaction->commit (cb => sub {
        $called++ if $_[1]->is_error;
        $cv->end;
      });
      undef $transaction;
    }) if $_[1]->is_success;
    $cv->end if $_[1]->is_error;
  });

  $cv->end;
  $cv->recv;

  is $called, 2;
  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
} # _execute_transaction_nested_error_2

sub _execute_transaction_commit_twice : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int primary key) engine=innodb');
  $cv->begin;

  my $called;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $cv->end;
  });

  $cv->begin;
  $transaction->commit (cb => sub {
    $cv->end;
  });

  dies_here_ok {
    $transaction->commit (cb => sub {
      #
    });
  };

  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [{id => 1}, {id => 2}];
} # _execute_transaction_commit_twice

sub _execute_transaction_rollback_twice : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int primary key) engine=innodb');
  $cv->begin;

  my $called;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $cv->end;
  });

  $cv->begin;
  $transaction->rollback (cb => sub {
    $cv->end;
  });

  dies_here_ok {
    $transaction->rollback (cb => sub {
      #
    });
  };

  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
} # _execute_transaction_rollback_twice

sub _execute_transaction_commit_rollback : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int primary key) engine=innodb');
  $cv->begin;

  my $called;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $cv->end;
  });

  $cv->begin;
  $transaction->commit (cb => sub {
    $cv->end;
  });

  dies_here_ok {
    $transaction->rollback (cb => sub {
      #
    });
  };

  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [{id => 1}, {id => 2}];
} # _execute_transaction_commit_rollback

sub _execute_transaction_rollback_commit : Test(2) {
  my $db = new_db;
  $db->source ('master')->{anyevent} = 1;

  my $cv = AnyEvent->condvar;

  $db->execute ('create table foo (id int primary key) engine=innodb');
  $cv->begin;

  my $called;

  my $transaction = $db->transaction;
  $cv->begin;
  $db->execute ('insert into foo (id) values (1), (2)', undef, cb => sub {
    $cv->end;
  });

  $cv->begin;
  $transaction->rollback (cb => sub {
    $cv->end;
  });

  dies_here_ok {
    $transaction->commit (cb => sub {
      #
    });
  };

  $cv->end;
  $cv->recv;

  my $result = $db->execute
      ('select * from foo order by id', undef, source_name => 'default');
  eq_or_diff $result->all->to_a, [];
} # _execute_transaction_rollback_commit

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
