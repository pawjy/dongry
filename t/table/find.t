package test::Dongry::Table::find;
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

# ------ |find| and |find_all| ------

sub _find_parsable_1 : Test(7) {
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

  my $row = $db->table ('table1')->find
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"});
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2001-04-01T00:12:50';
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col1 => DateTime->new (year => 2001, month => 4, day => 1,
                               hour => 0, minute => 12, second => 50),
        col2 => \"abc def"});
  isa_list_n_ok $row_list, 1;
  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is_datetime $row_list->[0]->get ('col1'), '2001-04-01T00:12:50';
  is ${$row_list->[0]->get ('col2')}, 'abc def';
} # _find_parsable_1

sub _find_parsable_many : Test(13) {
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
  $db->execute ('insert into table1 (col1, col2)
                 values ("2001-05-01 00:12:50", "abc def")');
  $db->execute ('insert into table1 (col1, col2)
                 values ("2002-04-01 00:12:50", "abc def")');

  my $row = $db->table ('table1')->find
      ({col2 => \"abc def"},
       order => [col1 => 1]);
  isa_ok $row, 'Dongry::Table::Row';
  is_datetime $row->get ('col1'), '2001-04-01T00:12:50';
  is ${$row->get ('col2')}, 'abc def';

  my $row_list = $db->table ('table1')->find_all
      ({col2 => \"abc def"}, order => [col1 => 1]);
  isa_list_n_ok $row_list, 3;

  isa_ok $row_list->[0], 'Dongry::Table::Row';
  is_datetime $row_list->[0]->get ('col1'), '2001-04-01T00:12:50';
  is ${$row_list->[0]->get ('col2')}, 'abc def';

  isa_ok $row_list->[1], 'Dongry::Table::Row';
  is_datetime $row_list->[1]->get ('col1'), '2001-05-01T00:12:50';
  is ${$row_list->[1]->get ('col2')}, 'abc def';

  isa_ok $row_list->[2], 'Dongry::Table::Row';
  is_datetime $row_list->[2]->get ('col1'), '2002-04-01T00:12:50';
  is ${$row_list->[2]->get ('col2')}, 'abc def';
} # _find_parsable_many

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
