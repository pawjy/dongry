package test::Dongry::Database::anyevent::forcesource;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use AnyEvent;

sub _force_source_name : Test(6) {
  my $db1 = new_db;
  my $db2 = new_db;
  my $db = Dongry::Database->new
      (sources => {ae1 => {dsn => $db1->source ('master')->{dsn},
                           anyevent => 1, writable => 1},
                   ae2 => {dsn => $db2->source ('master')->{dsn},
                           anyevent => 1, writable => 1}});
  $db1->execute ('create table foo (id int, value blob) engine=innodb');
  $db2->execute ('create table foo (id int, value blob) engine=innodb');

  $db1->execute ('insert into foo (id, value) values (1, "a")');
  $db2->execute ('insert into foo (id, value) values (1, "b")');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result1;
  $cv->begin;
  my $fs = $db->force_source_name ('ae1');
  $db->execute ('select * from foo', undef, cb => sub {
    $result1 = $_[1];
    $cv->end;
  });
  $fs->end;

  my $result2;
  $cv->begin;
  $fs = $db->force_source_name ('ae2');
  $db->execute ('select * from foo', undef, cb => sub {
    $result2 = $_[1];
    $cv->end;
  });

  $cv->end;
  $cv->recv;

  isa_ok $result1, 'Dongry::Database::Executed';
  is $result1->row_count, 1;
  eq_or_diff $result1->first, {id => 1, value => 'a'};

  isa_ok $result2, 'Dongry::Database::Executed';
  is $result2->row_count, 1;
  eq_or_diff $result2->first, {id => 1, value => 'b'};
} # _force_source_name

sub _force_source_name_error : Test(2) {
  my $db1 = new_db;
  my $db2 = new_db;
  my $db = Dongry::Database->new
      (sources => {ae1 => {dsn => $db1->source ('master')->{dsn},
                           anyevent => 1, writable => 1},
                   ae2 => {dsn => $db2->source ('master')->{dsn},
                           anyevent => 1, writable => 1}});
  $db1->execute ('create table foo (id int, value blob) engine=innodb');
  $db2->execute ('create table foo (id int, value blob) engine=innodb');

  $db1->execute ('insert into foo (id, value) values (1, "a")');
  $db2->execute ('insert into foo (id, value) values (1, "b")');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result1;
  my $fs = $db->force_source_name ('ae1');
  dies_here_ok {
    $db->execute ('select * from foo', undef, cb => sub {
      $result1 = $_[1];
    }, source_name => 'ae2');
  };
  $fs->end;

  $cv->end;
  $cv->recv;

  is $result1, undef;
} # _force_source_name_error

sub _force_source_name_next : Test(6) {
  my $db1 = new_db;
  my $db2 = new_db;
  my $db = Dongry::Database->new
      (sources => {ae1 => {dsn => $db1->source ('master')->{dsn},
                           anyevent => 1, writable => 1},
                   default => {dsn => $db2->source ('master')->{dsn},
                               anyevent => 1, writable => 1}});
  $db1->execute ('create table foo (id int, value blob) engine=innodb');
  $db2->execute ('create table foo (id int, value blob) engine=innodb');

  $db1->execute ('insert into foo (id, value) values (1, "a")');
  $db2->execute ('insert into foo (id, value) values (1, "b")');

  my $cv = AnyEvent->condvar;
  $cv->begin;

  my $result1;
  $cv->begin;
  my $fs = $db->force_source_name ('ae1');
  $db->execute ('select * from foo', undef, cb => sub {
    $result1 = $_[1];
    $cv->end;
  });
  undef $fs;

  my $result2;
  $cv->begin;
  $db->execute ('select * from foo', undef, cb => sub {
    $result2 = $_[1];
    $cv->end;
  });

  $cv->end;
  $cv->recv;

  isa_ok $result1, 'Dongry::Database::Executed';
  is $result1->row_count, 1;
  eq_or_diff $result1->first, {id => 1, value => 'a'};

  isa_ok $result2, 'Dongry::Database::Executed';
  is $result2->row_count, 1;
  eq_or_diff $result2->first, {id => 1, value => 'b'};
} # _force_source_name_next

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
