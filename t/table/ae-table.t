package test::Dongry::Table::anyevent::table;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use AnyEvent;
use Dongry::Type::Time;

sub _insert_cb : Test(9) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result;
  $db->table ('foo')->insert
      ([{id => 123, value => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->row_count, 2;
  is $result->table_name, 'foo';
  eq_or_diff $result->all->to_a,
      [{id => 123, value => '1970-01-07 06:37:02'},
       {id => 52, value => '1986-11-14 06:22:13'}];
} # _insert_cb

sub _insert_cb_exception : Test(2) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  $db->table ('foo')->insert
      ([{id => 123, value => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    die "abc";
    $cv->send;
  });

  eval {
    $cv->recv;
    ng 1;
  };
  like $@, qr{^abc at };

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a,
      [{id => 123, value => '1970-01-07 06:37:02'},
       {id => 52, value => '1986-11-14 06:22:13'}];
} # _insert_cb_exception

sub _insert_cb_error : Test(8) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result;
  $db->table ('foo')->insert
      ([{id => 123, value2 => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->send;
  });

  $cv->recv;

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a, [];

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{value2};
  ok $result->error_sql;
  dies_here_ok { $result->row_count };
} # _insert_cb_error

sub _insert_cb_exception_error : Test(3) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  $db->table ('foo')->insert
      ([{id => 123, value2 => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    die "abc";
    $cv->send;
  });

  eval {
    $cv->recv;
  };
  ok defined $@;
  like $warn, qr{^abc at};

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a, [];
} # _insert_cb_exception_error

sub _insert_cb_return : Test(7) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;

  my $cv = AnyEvent->condvar;

  my $result = $db->table ('foo')->insert
      ([{id => 123, value => 542222}, {id => 52, value => 532333333}],
       source_name => 'ae', cb => sub {
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  dies_here_ok { $result->all };

  eq_or_diff $db->select ('foo', {id => {-gt => 1}},
                          order => [id => -1])->all->to_a,
      [{id => 123, value => '1970-01-07 06:37:02'},
       {id => 52, value => '1986-11-14 06:22:13'}];
} # _insert_cb_return

sub _find_cb : Test(8) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $result;
  $db->table ('foo')->find ({id => {-gt => 4}},
                            order => [id => 1],
                            source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->table_name, 'foo';
  eq_or_diff $result->all->to_a, [{id => 12, value => '2012-01-01 00:12:12'}];
} # _find_cb

sub _find_all_cb : Test(8) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $result;
  $db->table ('foo')->find_all ({id => {-gt => 4}},
                                order => [id => 1],
                                source_name => 'ae', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->send;
  });

  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->table_name, 'foo';
  eq_or_diff $result->all->to_a,
      [{id => 12, value => '2012-01-01 00:12:12'},
       {id => 21, value => '1991-02-12 12:12:01'}];
} # _find_all_cb

sub _find_cb_return : Test(4) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  dies_here_ok {
    my $return = $db->table ('foo')->find ({id => {-gt => 4}},
                                           order => [id => 1],
                                           source_name => 'ae', cb => sub {
      $result = $_[1];
      $invoked++;
      $cv->send;
    });
  };

  $cv->recv;
  is $invoked, 1;
  is $result->row_count, 1;
  eq_or_diff $result->all->to_a, 
      [{id => 12, value => '2012-01-01 00:12:12'}];
} # _find_cb_return

sub _find_all_cb_return : Test(4) {
  my $db = new_db schema => {
    foo => {
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (12, "2012-01-01 00:12:12"),
                     (21, "1991-02-12 12:12:01")');

  my $cv = AnyEvent->condvar;

  my $invoked;
  my $result;
  dies_here_ok {
    my $return = $db->table ('foo')->find_all ({id => {-gt => 4}},
                                               order => [id => 1],
                                               source_name => 'ae', cb => sub {
      $result = $_[1];
      $invoked++;
      $cv->send;
    });
  };

  $cv->recv;
  is $invoked, 1;
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a, 
      [{id => 12, value => '2012-01-01 00:12:12'},
       {id => 21, value => '1991-02-12 12:12:01'}];
} # _find_all_cb_return

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
