package test::Dongry::Table::get;
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

# ------ |get| and |get_bare| ------

sub _get_parsable : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is_datetime $date1, '2001-04-01T00:12:50';
  my $date2 = $row->get ('col1');
  is $date2, $date1;

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_parsable

sub _get_not_parsable : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, '2001-04-01 00:12:50';
  ng ref $date1;

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_not_parsable

sub _get_undef : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1
                  (col1 blob default null, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values (NULL, "abc def")');

  my $row = $db->select ('table1', {col1 => undef})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, undef;

  my $str1 = $row->get_bare ('col1');
  is $str1, undef;
} # _get_undef

sub _get_broken : Test(3) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1
                  (col1 blob default null, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("xyxz abc ee-11-11", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_ok {
    my $date1 = $row->get ('col1');
  };
  dies_ok {
    my $date2 = $row->get ('col1');
  };

  my $str1 = $row->get_bare ('col1');
  is $str1, 'xyxz abc ee-11-11';
} # _get_broken

sub _get_column_defined_but_not_found : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col3, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col3 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, undef;

  my $str1 = $row->get_bare ('col1');
  is $str1, undef;
} # _get_column_defined_but_not_found

sub _get_column_not_found : Test(2) {
  my $schema = {
    table1 => {
      type => {
        col3 => 'timestamp_as_DateTime',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col3, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col3 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, undef;

  my $str1 = $row->get_bare ('col1');
  is $str1, undef;
} # _get_column_not_found

sub _get_column_unknown_type : Test(3) {
  my $schema = {
    table1 => {
      type => {
        col1 => 'as_unknown',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  dies_ok {
    my $date1 = $row->get ('col1');
  };

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_column_unknown_type

sub _get_column_no_schema : Test(4) {
  my $schema = {
    table2 => {
      type => {
        col1 => 'as_unknown',
        col2 => 'as_ref',
      },
      _create => 'create table table1 (col1 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-04-01 00:12:50", "abc def")');

  my $row = $db->select ('table1', {col1 => {-not => undef}})->first_as_row;

  my $date1 = $row->get ('col1');
  is $date1, '2001-04-01 00:12:50';
  ng ref $date1;

  my $str1 = $row->get_bare ('col1');
  is $str1, '2001-04-01 00:12:50';
  ng ref $str1;
} # _get_column_no_schema

sub _get_data_from_insert : Test(4) {
  my $schema = {
    table1 => {
      type => {
        col3 => 'timestamp_as_DateTime',
      },
      _create => 'create table table1 (col3 timestamp, col2 blob)',
    },
  };
  my $db = new_db schema => $schema;
  
  my $date0 = DateTime->new (year => 2001, month => 4, day => 1);
  my $row = $db->table ('table1')->create
      ({col3 => $date0, col2 => 'abc def'});

  my $date1 = $row->get ('col3');
  is $date1, $date0;
  isa_ok $date1, 'DateTime';

  my $str1 = $row->get_bare ('col3');
  is $str1, '2001-04-01 00:00:00';
  ng ref $str1;
} # _get_data_from_insert

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
