package test::Dongry::Database::execute;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;

sub _execute_create_table_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute
      ('create table table1 (id int unsigned not null primary key)');
  
  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  lives_ok { $db2->execute ('select * from table1') };
} # _execute_create_table

sub _execute_create_table_no_return_no_master : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn, writable => 1}});

  dies_ok {
    $db->execute
        ('create table table1 (id int unsigned not null primary key)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  dies_ok { $db2->execute ('select * from table1') };
} # _execute_create_table_no_master

sub _execute_create_table_not_writable : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 0}});

  dies_ok {
    $db->execute
        ('create table table1 (id int unsigned not null primary key)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  dies_ok { $db2->execute ('select * from table1') };
} # _execute_create_table_not_writable

sub _execute_insert_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('insert into table1 (id) values (5)');
  
  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  is $db2->execute ('select * from table1')->row_count, 1;
} # _execute_insert_no_return

sub _execute_insert_no_return_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});

  $db->execute ('INSERT INTO table1 (id) values (5)');
  
  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  is $db2->execute ('select * from table1')->row_count, 1;
} # _execute_insert_no_return_2

sub _execute_insert_no_return_not_writable : Test(2) {
  reset_db_set;
  my $dsn = test_dsn 'createtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 0}});
  dies_ok {
    $db->execute ('insert into table1 (id) values (5)');
  };

  my $db2 = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  is $db2->execute ('select * from table1')->row_count, 0;
} # _execute_insert_no_return

sub _execute_select_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  lives_ok {
    $db->execute ('select * from table1');
  };
} # _execute_select_no_return

sub _execute_select_no_return_2 : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});

  lives_ok {
    $db->execute ('SELECT * FROM table1');
  };
} # _execute_select_no_return_2

sub _execute_select_no_return_no_default : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {test => {dsn => $dsn}});

  dies_ok {
    $db->execute ('select * from table1');
  };
} # _execute_select_no_return_no_default

sub _execute_show_tables_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  lives_ok {
    $db->execute ('show tables');
  };
} # _execute_show_tables_no_return

sub _execute_explain_no_return : Test(1) {
  reset_db_set;
  my $dsn = test_dsn 'testtable';
  my $db0 = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1}});
  $db0->execute
      ('create table table1 (id int unsigned not null primary key)');

  my $db = Dongry::Database->new
      (sources => {default => {dsn => $dsn}});
  lives_ok {
    $db->execute ('explain select * from table1');
  };
} # _execute_explain_no_return

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
