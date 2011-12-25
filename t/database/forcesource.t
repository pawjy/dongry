package test::Dongry::Database::forcesource;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _force_source : Test(4) {
  my $db1 = new_db;
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');

  my $db2 = new_db;
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (2)');

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('default'),
                   default => $db2->source ('default')});
  
  my $force = $db->force_source_name ('master');
  is $db->execute ('select * from foo')->first->{id}, 1;
  $force->end;

  is $db->execute ('select * from foo')->first->{id}, 2;

  lives_ok { $force->end };
  lives_ok { undef $force };
} # _force_source

sub _force_source_destroy : Test(2) {
  my $db1 = new_db;
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');

  my $db2 = new_db;
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (2)');

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('default'),
                   default => $db2->source ('default')});
  
  my $force = $db->force_source_name ('master');
  is $db->execute ('select * from foo')->first->{id}, 1;
  undef $force;

  is $db->execute ('select * from foo')->first->{id}, 2;
} # _force_source_destroy

sub _force_source_2 : Test(3) {
  my $db1 = new_db;
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');

  my $db2 = new_db;
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (2)');

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('master'),
                   default => $db2->source ('master')});
  
  my $force = $db->force_source_name ('default');
  $db->execute ('insert into foo (id) value (3)');
  $force->end;

  is $db->execute ('select * from foo where id = 3')->first->{id}, 3;

  lives_ok { $force->end };
  lives_ok { undef $force };
} # _force_source_2

sub _force_source_vs_explicit : Test(2) {
  my $db1 = new_db;
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');

  my $db2 = new_db;
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (2)');

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('default'),
                   default => $db2->source ('default')});
  
  my $force = $db->force_source_name ('master');
  is $db->execute ('select * from foo', undef,
                   source_name => 'master')->first->{id}, 1;
  dies_here_ok {
    $db->execute ('select * from foo', undef, source_name => 'default');
  };
} # _force_source_vs_explicit

sub _force_source_in_another : Test(2) {
  my $db1 = new_db;
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');

  my $db2 = new_db;
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (2)');

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('default'),
                   default => $db2->source ('default')});
  
  my $force = $db->force_source_name ('master');
  dies_here_ok {
    $force = $db->force_source_name ('default');
  };
  is $db->execute ('select * from foo')->first->{id}, 1;
} # _force_source_in_another

sub _force_source_then_transaction : Test(2) {
  my $db1 = new_db;
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');

  my $db2 = new_db;
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (2)');

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('default'),
                   default => $db2->source ('default')});
  
  my $force = $db->force_source_name ('default');
  dies_here_ok {
    my $transaction = $db->transaction;
  };
  is $db->execute ('select * from foo')->first->{id}, 2;
} # _force_source_then_transaction

sub _force_source_in_transaction : Test(2) {
  my $db1 = new_db;
  $db1->execute ('create table foo (id int)');
  $db1->execute ('insert into foo (id) values (1)');

  my $db2 = new_db;
  $db2->execute ('create table foo (id int)');
  $db2->execute ('insert into foo (id) values (2)');

  my $db = Dongry::Database->new
      (sources => {master => $db1->source ('default'),
                   default => $db2->source ('default')});
  
  my $transaction = $db->transaction;
  dies_here_ok {
    my $force = $db->force_source_name ('default');
  };
  is $db->execute ('select * from foo')->first->{id}, 1;
  $transaction->commit;
} # _force_source_in_transaction

sub _debug_info : Test(1) {
  my $db = Dongry::Database->new;
  my $force = $db->force_source_name ('master');
  is $force->debug_info, '{DBForceSource: master}';
} # debug_info

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
