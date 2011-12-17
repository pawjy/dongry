package test::Dongry::Table::set;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Type::DateTime;
use Encode;

sub new_db (%) {
  my %args = @_;
  reset_db_set;
  my $dsn = test_dsn 'test1';
  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, writable => 1},
                   default => {dsn => $dsn}},
       schema => $args{schema});
  for my $name (keys %{$args{schema} || {}}) {
    if ($args{schema}->{$name}->{_create}) {
      $db->execute ($args{schema}->{$name}->{_create});
    }
  }
  return $db;
} # new_db

# ------ |set| ------

sub _set_parsable : Test(1) {
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
  $row->set ({col1 => $date1});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2008-10-03 00:00:00', col2 => 'abc def'};
} # _get_parsable

sub _set_unparsable : Test(1) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => [qw/col2/],
      _create => 'create table table1 (col1 timestamp default 0,
                                       col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  $row->set ({col3 => 'abc xyz'});

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def', col3 => 'abc xyz'};
} # _get_unparsable

sub _set_unparsable_but_ref : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      primary_keys => [qw/col2/],
      _create => 'create table table1 (col1 timestamp default 0,
                                       col2 blob, col3 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_ok {
    $row->set ({col3 => \'abc xyz'});
  };

  my $data = $db->select ('table1', {col1 => {-not => undef}})->first;
  eq_or_diff $data,
      {col1 => '2001-04-01 00:12:50', col2 => 'abc def', col3 => undef};
} # _get_unparsable_but_ref

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
