package test::Dongry::Table::Row::anyevent;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use Test::MoreMore::Mock;
use base qw(Test::Class);
use AnyEvent;
use Dongry::Type::Time;

sub _reload_cb : Test(9) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});
  $db->execute ('update foo set value = "2001-05-12 01:12:11"');

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->reload (cb => sub { 
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $row->get ('value'), '989629931';
  is $row->get_bare ('value'), '2001-05-12 01:12:11';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _reload_cb

sub _reload_cb_return : Test(10) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});
  $db->execute ('update foo set value = "2001-05-12 01:12:11"');

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  my $row2 = $row->reload (cb => sub { 
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
    $cv->send;
  }, source_name => 'ae');
  is $row2, $row;

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $row->get ('value'), '989629931';
  is $row->get_bare ('value'), '2001-05-12 01:12:11';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _reload_cb_return

sub _reload_cb_error : Test(9) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});
  $db->execute ('drop table foo');

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->reload (cb => sub { 
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{foo};
  like $result->error_sql, qr{foo};
  is $row->get ('value'), '1325679120';
  is $row->get_bare ('value'), '2012-01-04 12:12:00';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _reload_cb_error

sub _reload_cb_exception : Test(3) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});
  $db->execute ('update foo set value = "2001-05-12 01:12:11"');

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->reload (cb => sub { 
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;

  ok not $@;
  is $row->get ('value'), '989629931';
  is $row->get_bare ('value'), '2001-05-12 01:12:11';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _reload_cb_exception

sub _reload_cb_exception_error : Test(4) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});
  $db->execute ('drop table foo');

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->reload (cb => sub { 
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;

  ok not $@;
  like $warn, qr{^Died within handler: abc at };
  is $row->get ('value'), '1325679120';
  is $row->get_bare ('value'), '2012-01-04 12:12:00';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _reload_cb_exception_error

sub _update_cb : Test(9) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->update ({value => 989629931}, cb => sub { 
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $row->get ('value'), '989629931';
  is $row->get_bare ('value'), '2001-05-12 01:12:11';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _update_cb

sub _update_cb_error : Test(9) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});
  $db->execute ('drop table foo');

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->update ({value => 14331134}, cb => sub { 
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
    $cv->send;
  }, source_name => 'ae');

  $cv->recv;

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{foo};
  like $result->error_sql, qr{foo};
  is $row->get ('value'), '1325679120';
  is $row->get_bare ('value'), '2012-01-04 12:12:00';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _update_cb_error

sub _update_cb_exception : Test(3) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->update ({value => 989629931}, cb => sub { 
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;

  ok not $@;
  is $row->get ('value'), '989629931';
  is $row->get_bare ('value'), '2001-05-12 01:12:11';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _update_cb_exception

sub _update_cb_exception_error : Test(4) {
  my $db = new_db schema => {
    foo => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'create table foo (id int, value timestamp)',
    },
  }, ae => 1;
  $db->execute ('insert into foo (id, value) values
                     (124, "2012-01-04 12:12:00")');

  my $row = $db->table ('foo')->find ({id => 124});
  $db->execute ('drop table foo');

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;

  my $result;
  my $invoked;
  $row->update ({value => 4442131}, cb => sub { 
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->recv;

  ok not $@;
  like $warn, qr{^Died within handler: abc at };
  is $row->get ('value'), '1325679120';
  is $row->get_bare ('value'), '2012-01-04 12:12:00';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _update_cb_exception_error

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
