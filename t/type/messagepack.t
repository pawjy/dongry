package test::Dongry::Type::MessagePack;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Type::MessagePack;

sub _version : Test(1) {
  ok $Dongry::Type::MessagePack::VERSION;
} # _version

# ------ messagepack ------

sub _messagepack_valid : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'messagepack'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123,
                   value => '{"hoge":{"fuga":[1,2,"abc"]},"fuga":null}'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  eq_or_diff $value, undef;

  ## Serialize
  $row->update ({value => {abc => ["\x{5000}"], xyz => "\xA1\xFE\x89"}});
  $row->reload;
  my $value2 = $row->get ('value');
  eq_or_diff $value2, {abc => ["\345\200\200"], xyz => "\xA1\xFE\x89"};
} # _messagepack_valid

sub _messagepack_primitive : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'messagepack'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123,
                   value => '1245'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  eq_or_diff $value, undef;

  ## Serialize
  $row->update ({value => "abc def"});
  $row->reload;
  my $value2 = $row->get ('value');
  eq_or_diff $value2, "abc def";
} # _messagepack_primitive

sub _messagepack_empty : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'messagepack'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123,
                   value => ''}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  eq_or_diff $value, undef;

  ## Serialize
  $row->update ({value => ""});
  $row->reload;
  my $value2 = $row->get ('value');
  eq_or_diff $value2, '';
} # _messagepack_primitive

sub _messagepack_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'messagepack'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123,
                   value => undef}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  eq_or_diff $value, undef;

  ## Serialize
  $row->update ({value => undef});
  $row->reload;
  my $value2 = $row->get ('value');
  eq_or_diff $value2, undef;
  is $row->get_bare ('value'), undef;
} # _messagepack_undef

sub _messagepack_undef_2 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'messagepack'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123,
                   value => 'null'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  eq_or_diff $value, undef;

  ## Serialize
  $row->update ({value => undef});
  $row->reload;
  my $value2 = $row->get ('value');
  eq_or_diff $value2, undef;
  is $row->get_bare ('value'), undef;
} # _messagepack_undef_2

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
