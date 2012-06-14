package test::Dongry::Query;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

sub _find_null_cb : Test(7) {
  my $db = Dongry::Database->new;
  my $filtered;
  my $q = $db->query (item_list_filter => sub {
    $filtered++;
    return $_[1]->map (sub { [$_->get ('id') + 1] });
  });

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  $q->find (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;
  
  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $value, undef;
} # _find_null_cb

sub _find_all_null_cb : Test(7) {
  my $db = Dongry::Database->new;
  my $filtered;
  my $q = $db->query (item_list_filter => sub {
    $filtered++;
    return $_[1]->map (sub { [$_->get ('id') + 1] });
  });

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  $q->find_all (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;
  
  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  isa_list_n_ok $value, 0;
} # _find_all_null_cb

sub _count_null_cb : Test(7) {
  my $db = Dongry::Database->new;
  my $filtered;
  my $q = $db->query (item_list_filter => sub {
    $filtered++;
    return $_[1]->map (sub { [$_->get ('id') + 1] });
  });

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  $q->count (cb => sub {
    $invoked++;
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $cv->send;
  });

  $cv->recv;
  
  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  is $value, 0;
} # _count_null_cb

sub _find_filtered_cb : Test(7) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  $q->find (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  is $filtered, 1;
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
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  $q->find_all (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  is $filtered, 1;
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
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  $q->count (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  ng $filtered;
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
  }, ae => 1;

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  $q->count (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, 0;
} # _count_filtered_cb_zero

sub _find_filtered_cb_return : Test(8) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  dies_here_ok {
    my $return = $q->find (cb => sub {
      is $_[0], $db;
      $result = $_[1];
      $value = $_;
      $invoked++;
      $cv->send;
    }, source_name => 'ae');
  };

  $cv->recv;
  
  is $filtered, 1;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, [2];
} # _find_filtered_cb_return

sub _find_all_filtered_cb_return : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  dies_here_ok {
    my $return = $q->find_all (cb => sub {
      is $_[0], $db;
      $result = $_[1];
      $value = $_;
      $invoked++;
      $cv->send;
    }, source_name => 'ae');
  };

  $cv->recv;
  
  is $filtered, 1;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  isa_list_n_ok $value, 3;
  eq_or_diff $value->to_a, [[2], [3], [4]];
} # _find_all_filtered_cb_return

sub _count_filtered_cb_return : Test(8) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  dies_here_ok {
    my $return = $q->count (cb => sub {
      is $_[0], $db;
      $result = $_[1];
      $value = $_;
      $invoked++;
      $cv->send;
    }, source_name => 'ae');
  };

  $cv->recv;
  
  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, 3;
} # _count_filtered_cb_return

sub _count_filtered_cb_return_zero : Test(8) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $result;
  my $value;
  my $invoked;
  dies_here_ok {
    my $return = $q->count (cb => sub {
      is $_[0], $db;
      $result = $_[1];
      $value = $_;
      $invoked++;
      $cv->send;
    }, source_name => 'ae');
  };

  $cv->recv;
  
  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  eq_or_diff $value, 0;
} # _count_filtered_cb_return_zero

sub _find_filtered_cb_error : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  $q->find (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{id2};
  ok $result->error_sql;
  is $value, undef;
} # _find_filtered_cb_error

sub _find_all_filtered_cb_error : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  $q->find_all (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{id2};
  ok $result->error_sql;
  is $value, undef;
} # _find_all_filtered_cb_error

sub _count_filtered_cb_error : Test(9) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  my $value;
  $q->count (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $value = $_;
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  ng $filtered;
  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{id2};
  ok $result->error_sql;
  is $value, undef;
} # _count_filtered_cb_error

sub _find_filtered_cb_exception : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  $q->find (cb => sub {
    die "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
    ng 1;
  };

  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 8]}\E\.?\n$};
} # _find_filtered_cb_exception

sub _find_all_filtered_cb_exception : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  $q->find_all (cb => sub {
    die "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
    ng 1;
  };

  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 8]}\E\.?\n$};
} # _find_all_filtered_cb_exception

sub _count_filtered_cb_exception : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  $q->count (cb => sub {
    die "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
    ng 1;
  };

  like $@, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 8]}\E\.?\n$};
} # _count_filtered_cb_exception

sub _find_filtered_cb_exception_carp : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  $q->find (cb => sub {
    Carp::croak "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
    ng 1;
  };

  like $@, qr{^abc at };
} # _find_filtered_cb_exception_carp

sub _find_all_filtered_cb_exception_carp : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  $q->find_all (cb => sub {
    Carp::croak "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
    ng 1;
  };

  like $@, qr{^abc at };
} # _find_all_filtered_cb_exception_carp

sub _count_filtered_cb_exception_carp : Test(1) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $q = $db->query
      (table_name => 'table1',
       where => {id => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $cv = AnyEvent->condvar;

  $q->count (cb => sub {
    Carp::croak "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
    ng 1;
  };

  like $@, qr{^abc at };
} # _count_filtered_cb_exception_carp

sub _find_filtered_cb_error_exception : Test(3) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  $q->find (cb => sub {
    die "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
  };

  ng $filtered;
  ok defined $@;
  like $warn, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 9]}\E\.?\n$};
} # _find_filtered_cb_error_exception

sub _find_all_filtered_cb_error_exception : Test(3) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  $q->find_all (cb => sub {
    die "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
  };

  ng $filtered;
  ok defined $@;
  like $warn, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 9]}\E\.?\n$};
} # _find_all_filtered_cb_error_exception

sub _count_filtered_cb_error_exception : Test(3) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  $q->count (cb => sub {
    die "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
  };

  ng $filtered;
  ok defined $@;
  like $warn, qr{^abc at \Q@{[__FILE__]} line @{[__LINE__ - 9]}\E\.?\n$};
} # _count_filtered_cb_error_exception

sub _find_filtered_cb_error_exception_carp : Test(3) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  $q->find (cb => sub {
    Carp::croak "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
  };

  ng $filtered;
  ok defined $@;
  like $warn, qr{abc at };
} # _find_filtered_cb_error_exception_carp

sub _find_all_filtered_cb_error_exception_carp : Test(3) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  $q->find_all (cb => sub {
    Carp::croak "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
  };

  ng $filtered;
  ok defined $@;
  like $warn, qr{abc at };
} # _find_all_filtered_cb_error_exception_carp

sub _count_filtered_cb_error_exception_carp : Test(3) {
  my $db = new_db schema => {
    table1 => {
      _create => 'CREATE TABLE table1 (id INT)',
    },
  }, ae => 1;
  $db->table ('table1')->insert ([{id => 1}, {id => 2}, {id => 3}]);

  my $filtered;
  my $q = $db->query
      (table_name => 'table1',
       where => {id2 => {-gt => 0}},
       order => [id => 1],
       item_list_filter => sub {
         $filtered++;
         return $_[1]->map (sub { [$_->get ('id') + 1] });
       });

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  $q->count (cb => sub {
    Carp::croak "abc";
  }, source_name => 'ae');

  eval {
    $cv->recv;
  };

  ng $filtered;
  ok defined $@;
  like $warn, qr{abc at };
} # _count_filtered_cb_error_exception_carp

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
