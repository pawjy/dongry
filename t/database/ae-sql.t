package test::Dongry::Database::anyevent::sql;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

# ------ insert ------

sub _insert_cb : Test(10) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->insert ('foo', [{id => 12}, {id => 21}], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->table_name, 'foo';
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a, [{id => 12}, {id => 21}];

  my $result2 = $db->execute ('select * from foo order by id asc');
  eq_or_diff $result2->all->to_a, [{id => 12}, {id => 21}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb

sub _insert_cb_error : Test(9) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->insert ('foo', [{id => 12}, {notid => 21}], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{notid};
  like $result->error_sql, qr{notid};
  ng $result->table_name;
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->all };

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb_error

sub _insert_cb_return : Test(6) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  $cv->begin;
  my $result2;
  my $result = $db->insert ('foo', [{id => 12}, {id => 21}], cb => sub {
    $result2 = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->all };
  isnt $result2, $result;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _insert_cb_return

sub _insert_cb_exception : Test(1) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->insert ('foo', [{id => 12}, {id => 21}], cb => sub {
    die "ab cd";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  ok not $@;
} # _insert_cb_exception

sub _insert_cb_exception_carp : Test(1) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->insert ('foo', [{id => 12}, {id => 21}], cb => sub {
    Carp::croak "ab cd";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  ok not $@;
} # _insert_cb_exception_carp

sub _insert_cb_error_exception : Test(2) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->insert ('foo', [{id => 12}, {notid => 21}], cb => sub {
    die "ab cd";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  ok not $@;
  is $warn, 'Died within handler: ab cd at ' . __FILE__ . ' line ' . (__LINE__ - 14) . ".\n";
} # _insert_cb_error_exception

sub _insert_cb_error_exception_carp : Test(2) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->insert ('foo', [{id => 12}, {notid => 21}], cb => sub {
    Carp::croak "ab cd";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  ok not $@;
  like $warn, qr{^Died within handler: ab cd at }; ## Location is not helpful
} # _insert_cb_error_exception_carp

# ------ select ------

sub _select_cb : Test(9) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->select ('foo', {id => {-gt => 1}}, order => [id => 1], cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $result->table_name, 'foo';
  is $result->row_count, 2;
  eq_or_diff $result->all->to_a, [{id => 4}, {id => 34}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _select_cb

sub _select_cb_return : Test(8) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  $cv->begin;
  my $result = $db->select
      ('foo', {id => {-gt => 1}}, order => [id => 1], cb => sub {
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  ng $result->table_name;
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->first };

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _select_cb_return

sub _select_cb_exception : Test(1) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  $db->select ('foo', {id => {-gt => 1}}, order => [id => 1], cb => sub {
    die "fu ga";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  ok not $@;

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _select_cb_exception

sub _select_cb_exception_error : Test(2) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;
  $cv->begin;

  $db->select ('foo', {mid => {-gt => 1}}, order => [id => 1], cb => sub {
    die "fu ga";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  ok not $@;
  is $warn, 'Died within handler: fu ga at ' . __FILE__ . ' line ' . (__LINE__ - 10) . ".\n";

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _select_cb_exception_error

# ------ update ------

sub _update_cb : Test(10) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->update ('foo', {id => 54}, where => {id => 4}, cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  ng $result->table_name;
  is $result->row_count, 1;
  dies_here_ok { $result->all };

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 34}, {id => 54}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _update_cb

sub _update_cb_return : Test(8) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  $cv->begin;
  my $result = $db->update ('foo', {id => 54}, where => {id => 4}, cb => sub {
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  ng $result->table_name;
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->all };

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _update_cb_return

sub _update_cb_error : Test(10) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->update ('foo', {xid => 54}, where => {id => 4}, cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{xid};
  like $result->error_sql, qr{xid};
  ng $result->table_name;
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->all };

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 4}, {id => 34}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _update_cb_error

sub _update_cb_exception : Test(2) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->update ('foo', {id => 54}, where => {id => 4}, cb => sub {
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  ok not $@;

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 34}, {id => 54}];
} # _update_cb_exception

sub _update_cb_exception_error : Test(3) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->update ('foo', {idx => 54}, where => {id => 4}, cb => sub {
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  ok not $@;
  is $warn, 'Died within handler: abc at ' . __FILE__ . ' line ' . (__LINE__ - 10) . ".\n";

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 4}, {id => 34}];
} # _update_cb_exception_error

# ------ delete ------

sub _delete_cb : Test(10) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->delete ('foo', {id => 4}, cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  ng $result->table_name;
  is $result->row_count, 1;
  dies_here_ok { $result->all };

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 34}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _delete_cb

sub _delete_cb_return : Test(8) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  $cv->begin;
  my $result = $db->delete ('foo', {id => 4}, cb => sub {
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  ng $result->table_name;
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->all };

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _delete_cb_return

sub _delete_cb_error : Test(10) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->delete ('foo', {xid => 4}, cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{xid};
  like $result->error_sql, qr{xid};
  ng $result->table_name;
  dies_here_ok { $result->row_count };
  dies_here_ok { $result->all };

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 4}, {id => 34}];

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _delete_cb_error

sub _delete_cb_exception : Test(2) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->delete ('foo', {id => 4}, cb => sub {
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  ok not $@;

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 34}];
} # _delete_cb_exception

sub _delete_cb_exception_error : Test(3) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});
  $db->execute ('create table foo (id int)');
  $db->execute ('insert into foo (id) values (34), (4)');

  my $warn;
  local $SIG{__WARN__} = sub { $warn = $_[0] };

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $db->delete ('foo', {xid => 4}, cb => sub {
    die "abc";
  }, source_name => 'ae');

  $cv->begin;
  $db->execute ('show tables', undef, cb => sub { $cv->end }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  $cv = AE::cv;
  $db->disconnect ('ae', cb => sub { $cv->send });
  $cv->recv;

  ok not $@;
  is $warn, 'Died within handler: abc at ' . __FILE__ . ' line ' . (__LINE__ - 14) . ".\n";

  eq_or_diff $db->execute ('select * from foo order by id asc')->all->to_a,
      [{id => 4}, {id => 34}];
} # _delete_cb_exception_error

# ------ set_tz ------

sub _set_tz_cb : Test(7) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->set_tz ('+01:00', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  my $tz;
  $cv->begin;
  $db->execute ('SELECT @@session.time_zone AS tz', undef, cb => sub {
    $tz = $_[1]->first->{tz};
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
  ng $result->error_text;
  ng $result->error_sql;
  is $tz, '+01:00';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _set_tz_cb

sub _set_tz_error : Test(7) {
  my $db = new_db;
  $db->source (ae => {dsn => $db->source ('master')->{dsn}, anyevent => 1,
                      writable => 1});

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result;
  $cv->begin;
  $db->set_tz ('ho ge', cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $cv->end;
  }, source_name => 'ae');

  my $tz;
  $cv->begin;
  $db->execute ('SELECT @@session.time_zone AS tz', undef, cb => sub {
    $tz = $_[1]->first->{tz} if $_[1]->is_success;
    $cv->end;
  }, source_name => 'ae');

  $cv->end;
  $cv->recv;

  isa_ok $result, 'Dongry::Database::Executed';
  ng $result->is_success;
  ok $result->is_error;
  like $result->error_text, qr{time zone};
  is $result->error_sql, 'SET time_zone = ?';
  isnt $tz, 'ho ge';

  $cv = AE::cv;
  $db->disconnect (undef, cb => sub { $cv->send });
  $cv->recv;
} # _set_tz_error

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012-2014 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
