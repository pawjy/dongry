package test::Dongry::Type::Storable;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Type::Storable;

sub _version : Test(1) {
  ok $Dongry::Type::Storable::VERSION;
} # _version

# ------ storable_nfreeze ------

sub _storable_nfreeze_valid : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'storable_nfreeze'},
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
  eq_or_diff $value2, {abc => ["\x{5000}"], xyz => "\xA1\xFE\x89"};
} # _storable_nfreeze_valid

sub _storable_nfreeze_primitive : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'storable_nfreeze'},
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
  eq_or_diff $value2, undef;
} # _storable_nfreeze_primitive

sub _storable_nfreeze_empty : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'storable_nfreeze'},
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
  eq_or_diff $value2, undef;
} # _storable_nfreeze_empty

sub _storable_nfreeze_code : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'storable_nfreeze'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123,
                   value => sub { 1243 }}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  eq_or_diff $value, undef;

  ## Serialize
  $row->update ({value => [sub { 44 }]});
  $row->reload;
  my $value2 = $row->get ('value');
  eq_or_diff $value2, undef;
} # _storable_nfreeze_code

sub _storable_nfreeze_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'storable_nfreeze'},
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
} # _storable_nfreeze_undef

sub _storable_nfreeze_undef_2 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'storable_nfreeze'},
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
} # _storable_nfreeze_undef_2

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Hatena <http://www.hatena.ne.jp/>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
