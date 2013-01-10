package test::Dongry::Table::delete;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Type::DateTime;
use Encode;

# ------ |update| ------

sub _delete : Test(1) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => [qw/col2/],
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = DateTime->new (year => 2008, month => 10, day => 3);
  $row->delete;

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  is $data, undef;
} # _delete

sub _delete_no_schema : Test(2) {
  my $db = new_db schema => undef;
  $db->execute ('create table table1 (col1 timestamp default 0, col2 blob)');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->delete;
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def'};
} # _delete_no_schema

sub _delete_no_row : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int primary key, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;
  $db->delete ('table1', {col1 => 120});

  dies_here_ok {
    $row->delete;
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  is $data, undef;
} # _delete_no_row

sub _delete_multiple_rows : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc xyz")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->delete;
  };

  eq_or_diff $db->select ('table1', {col1 => {-not => undef}})->all->to_a, [];
} # _delete_multiple_rows

sub _delete_not_writable : Test(3) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');
  $db->source ('master')->{writable} = 0;

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_here_ok {
    $row->delete;
  };

  is $row->get ('col2'), 'abc def';
  is $row->reload->get ('col2'), 'abc def';
} # _delete_not_writable

sub _delete_source_name : Test(1) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');
  $db->source ('master')->{writable} = 0;
  $db->source ('default')->{writable} = 1;

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->delete (source_name => 'default');

  is $db->select ('table1', {col1 => {-not => undef}})->first, undef;
} # _update_source_name

sub _delete_cb : Test(5) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $invoked;
  my $result;
  $row->delete (cb => sub {
    is $_[0], $db;
    $result = $_[1];
    $invoked++;
  });

  is $invoked, 1;
  isa_ok $result, 'Dongry::Database::Executed';
  ok $result->is_success;
  ng $result->is_error;
} # _delete_cb

sub _delete_cb_error : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;
  $db->execute ('drop table table1');

  my $invoked;
  dies_here_ok {
    $row->delete (cb => sub {
      $invoked++;
    });
  };

  ng $invoked;
} # _delete_cb_error

sub _delete_cb_exception : Test(2) {
  my $schema = {
    table1 => {
      primary_keys => [qw/col1/],
      _create => 'create table table1 (col1 int, col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (120, "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  eval {
    $row->delete (cb => sub {
      die "abc";
    });
    ng 1;
  };

  is $@, 'abc at ' . __FILE__ . ' line ' . (__LINE__ - 5) . ".\n";
  is $db->select ('table1', {col1 => {-not => undef}})->first, undef;
} # _delete_cb_exception

__PACKAGE__->runtests;

$Dongry::LeakTest = 1;

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
