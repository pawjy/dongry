package test::Dongry::Table::fill;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Test::MoreMore::Mock;
BEGIN { $DBIx::ShowSQL::WARN //= 0 };
use DBIx::ShowSQL;
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

# ------ |fill_related_rows| ------

sub _fill_related_rows_simple_1 : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_ok $mock1->related_row, 'Dongry::Table::Row';
  is $mock1->related_row->get ('id'), 12345;
} # _fill_related_rows_simple_1

sub _fill_related_rows_simple_1_replace : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);
  $mock1->related_row ('hogehoge');

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_ok $mock1->related_row, 'Dongry::Table::Row';
  is $mock1->related_row->get ('id'), 12345;
} # _fill_related_rows_simple_1_replace

sub _fill_related_rows_simple_not_found : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);
  $mock1->related_row ('hogehoge');

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row, undef;
} # _fill_related_rows_simple_not_found

sub _fill_related_rows_multiple : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 512});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 124);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 512);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id'), 124;
  is $mock2->related_row->get ('id'), 512;
} # _fill_related_rows_multiple

sub _fill_related_rows_multiple_blessed : Test(3) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 512});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 124);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 512);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ((bless [$mock1, $mock2], 'List::Rubyish')
       => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id'), 124;
  is $mock2->related_row->get ('id'), 512;
} # _fill_related_rows_multiple_blessed

sub _fill_related_rows_zero_list : Test(1) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 512});

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 0;
} # _fill_related_rows_zero_list

sub _fill_related_rows_multiple_results : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 124});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 124);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id'), 124;
} # _fill_related_rows_multiple_results

sub _fill_related_rows_multiple_same_items : Test(4) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 124);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 124);
  my $mock3 = Test::MoreMore::Mock->new (related_id => 124);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2, $mock3] => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id'), 124;
  is $mock2->related_row->get ('id'), 124;
  is $mock3->related_row->get ('id'), 124;
} # _fill_related_rows_multiple_same_items

sub _fill_related_rows_multiple_found_1 : Test(4) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 124});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 1;
  isa_ok $mock1->related_rows->[0], 'Dongry::Table::Row';
  is $mock1->related_rows->[0]->get ('id'), 12345;
} # _fill_related_rows_multiple_found_1

sub _fill_related_rows_multiple_found_2 : Test(6) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 12345});
  $table->create ({id => 12345});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 2;
  isa_ok $mock1->related_rows->[0], 'Dongry::Table::Row';
  is $mock1->related_rows->[0]->get ('id'), 12345;
  isa_ok $mock1->related_rows->[1], 'Dongry::Table::Row';
  is $mock1->related_rows->[1]->get ('id'), 12345;
} # _fill_related_rows_multiple_found_2

sub _fill_related_rows_multiple_found_0 : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id => 'id'} => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 0;
} # _fill_related_rows_multiple_found_0

sub _fill_related_rows_multiple_found_multiples : Test(7) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 12345});
  $table->create ({id => 12345});
  $table->create ({id => 12341});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 12341);
  my $mock3 = Test::MoreMore::Mock->new (related_id => 12347);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2, $mock3] => {related_id => 'id'} => 'related_rows',
       multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 2;
  is $mock1->related_rows->[0]->get ('id'), 12345;
  is $mock1->related_rows->[1]->get ('id'), 12345;

  isa_list_n_ok $mock2->related_rows, 1;
  is $mock2->related_rows->[0]->get ('id'), 12341;

  isa_list_n_ok $mock3->related_rows, 0;
} # _fill_related_rows_multiple_found_multiples

sub _fill_related_rows_multiple_keys_1 : Test(4) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id1 int, id2 int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id1 => 124, id2 => 12345});
  $table->create ({id1 => 12345, id2 => 124});

  my $mock1 = Test::MoreMore::Mock->new
      (related_id1 => 12345, related_id2 => 124);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id1 => 'id1', related_id2 => 'id2'}
       => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_ok $mock1->related_row, 'Dongry::Table::Row';
  is $mock1->related_row->get ('id1'), 12345;
  is $mock1->related_row->get ('id2'), 124;
} # _fill_related_rows_multiple_keys_1

sub _fill_related_rows_multiple_keys_3 : Test(11) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id1 int, id2 int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id1 => 124, id2 => 12345});
  $table->create ({id1 => 12345, id2 => 124});
  $table->create ({id1 => 12345, id2 => 124});
  $table->create ({id1 => 12345, id2 => 124});

  my $mock1 = Test::MoreMore::Mock->new
      (related_id1 => 12345, related_id2 => 124);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1] => {related_id1 => 'id1', related_id2 => 'id2'}
       => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 3;
  for (@{$mock1->related_rows}) {
    isa_ok $_, 'Dongry::Table::Row';
    is $_->get ('id1'), 12345;
    is $_->get ('id2'), 124;
  }
} # _fill_related_rows_multiple_keys_3

sub _fill_related_rows_multiple_keys_combinations : Test(19) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id1 int, id2 int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id1 => 124, id2 => 12345});
  $table->create ({id1 => 12345, id2 => 124});
  $table->create ({id1 => 12345, id2 => 124});
  $table->create ({id1 => 12345, id2 => 124});
  $table->create ({id1 => 12345, id2 => 126});
  $table->create ({id1 => 12345, id2 => 126});
  $table->create ({id1 => 12343, id2 => 126});

  my $mock1 = Test::MoreMore::Mock->new
      (related_id1 => 12345, related_id2 => 124);
  my $mock2 = Test::MoreMore::Mock->new
      (related_id1 => 12345, related_id2 => 126);
  my $mock3 = Test::MoreMore::Mock->new
      (related_id1 => 12343, related_id2 => 128);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2, $mock3] => {related_id1 => 'id1', related_id2 => 'id2'}
       => 'related_rows', multiple => 1);
  is $DBIx::ShowSQL::SQLCount, 1;

  isa_list_n_ok $mock1->related_rows, 3;
  for (@{$mock1->related_rows}) {
    isa_ok $_, 'Dongry::Table::Row';
    is $_->get ('id1'), 12345;
    is $_->get ('id2'), 124;
  }

  isa_list_n_ok $mock2->related_rows, 2;
  for (@{$mock2->related_rows}) {
    isa_ok $_, 'Dongry::Table::Row';
    is $_->get ('id1'), 12345;
    is $_->get ('id2'), 126;
  }

  isa_list_n_ok $mock3->related_rows, 0;
} # _fill_related_rows_multiple_keys_combinations

sub _fill_related_rows_multiple_keys_4 : Test(9) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id1 int, id2 int, id3 int, id4 int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id1 => 1, id2 => 2, id3 => 3, id4 => 4});
  $table->create ({id1 => 1, id2 => 2, id3 => 4, id4 => 3});
  $table->create ({id1 => 2, id2 => 1, id3 => 4, id4 => 3});
  $table->create ({id1 => 2, id2 => 1, id3 => 3, id4 => 4});

  my $mock1 = Test::MoreMore::Mock->new
      (related_id1 => 1, related_id2 => 2, related_id3 => 3, related_id4 => 4);
  my $mock2 = Test::MoreMore::Mock->new
      (related_id1 => 2, related_id2 => 1, related_id3 => 4, related_id4 => 3);

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2] => {related_id1 => 'id1', related_id2 => 'id2',
                            related_id3 => 'id3', related_id4 => 'id4'}
       => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 1;

  is $mock1->related_row->get ('id1'), 1;
  is $mock1->related_row->get ('id2'), 2;
  is $mock1->related_row->get ('id3'), 3;
  is $mock1->related_row->get ('id4'), 4;

  is $mock2->related_row->get ('id1'), 2;
  is $mock2->related_row->get ('id2'), 1;
  is $mock2->related_row->get ('id3'), 4;
  is $mock2->related_row->get ('id4'), 3;
} # _fill_related_rows_multiple_keys_4

sub _fill_related_rows_zero_methods : Test(2) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  my $mock1 = Test::MoreMore::Mock->new (related_id => 12345);

  local $DBIx::ShowSQL::SQLCount = 0;
  dies_here_ok {
    $table->fill_related_rows ([$mock1] => {} => 'related_row');
  };
  is $DBIx::ShowSQL::SQLCount, 0;
} # _fill_related_rows_zero_methods

sub _fill_related_rows_too_many : Test(5) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 1});
  $table->create ({id => 2});
  $table->create ({id => 3});
  $table->create ({id => 4});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 1);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 2);
  my $mock3 = Test::MoreMore::Mock->new (related_id => 3);
  my $mock4 = Test::MoreMore::Mock->new (related_id => 4);

  local $Dongry::Table::MaxFillItems = 3;

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2, $mock3, $mock4]
       => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 2;

  is $mock1->related_row->get ('id'), 1;
  is $mock2->related_row->get ('id'), 2;
  is $mock3->related_row->get ('id'), 3;
  is $mock4->related_row->get ('id'), 4;
} # _fill_related_rows_too_many

sub _fill_related_rows_too_many_2 : Test(8) {
  my $schema = {
    table1 => {
      _create => 'create table table1 (id int)',
    },
  };
  my $db = new_db schema => $schema;
  my $table = $db->table ('table1');

  $table->create ({id => 1});
  $table->create ({id => 2});
  $table->create ({id => 3});
  $table->create ({id => 4});
  $table->create ({id => 5});
  $table->create ({id => 6});
  $table->create ({id => 7});

  my $mock1 = Test::MoreMore::Mock->new (related_id => 1);
  my $mock2 = Test::MoreMore::Mock->new (related_id => 2);
  my $mock3 = Test::MoreMore::Mock->new (related_id => 3);
  my $mock4 = Test::MoreMore::Mock->new (related_id => 4);
  my $mock5 = Test::MoreMore::Mock->new (related_id => 5);
  my $mock6 = Test::MoreMore::Mock->new (related_id => 6);
  my $mock7 = Test::MoreMore::Mock->new (related_id => 7);

  local $Dongry::Table::MaxFillItems = 3;

  local $DBIx::ShowSQL::SQLCount = 0;
  $table->fill_related_rows
      ([$mock1, $mock2, $mock3, $mock4, $mock5, $mock6, $mock7]
       => {related_id => 'id'} => 'related_row');
  is $DBIx::ShowSQL::SQLCount, 3;

  is $mock1->related_row->get ('id'), 1;
  is $mock2->related_row->get ('id'), 2;
  is $mock3->related_row->get ('id'), 3;
  is $mock4->related_row->get ('id'), 4;
  is $mock5->related_row->get ('id'), 5;
  is $mock6->related_row->get ('id'), 6;
  is $mock7->related_row->get ('id'), 7;
} # _fill_related_rows_too_many_2

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
