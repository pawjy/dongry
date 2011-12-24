package test::Dongry::Type::basic;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Encode;

sub _version : Test(1) {
  ok $Dongry::Type::VERSION;
} # _version

sub _types : Test(1) {
  is ref $Dongry::Types, 'HASH';
} # _types

sub _as_ref : Test(4) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'hoge fuga'}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is ref $value, 'SCALAR';
  is $$value, 'hoge fuga';

  ## Serialize
  my $value1 = \'abc def';
  $row->update ({value => $value1});
  my $value2 = $row->reload->get ('value');
  isnt $value2, $value1;
  is $$value2, 'abc def';
} # _as_ref

sub _as_ref_bytes : Test(4) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "hoge fuga\x81\xFE\x34\xC9"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is ref $value, 'SCALAR';
  is $$value, "hoge fuga\x81\xFE\x34\xC9";

  ## Serialize
  my $value1 = \"abc \x81\xFE\x34\xC9def";
  $row->update ({value => $value1});
  my $value2 = $row->reload->get ('value');
  isnt $value2, $value1;
  is $$value2, "abc \x81\xFE\x34\xC9def";
} # _as_ref_bytes

sub _as_ref_chars : Test(4) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "hoge fuga\x{4112}\x{311}"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is ref $value, 'SCALAR';
  is $$value, encode 'utf-8', "hoge fuga\x{4112}\x{311}";

  ## Serialize
  my $value1 = \encode 'utf-8', "abc \x{4112}\x{311}def";
  $row->update ({value => $value1});
  my $value2 = $row->reload->get ('value');
  isnt $value2, $value1;
  is $$value2, encode 'utf-8', "abc \x{4112}\x{311}def";
} # _as_ref_chars

sub _as_ref_undef : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => undef}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  ## Serialize
  $row->update ({value => undef});
  my $value2 = $row->reload->get ('value');
  is $value2, undef;
} # _as_ref_undef

sub _as_ref_undef_ref : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => undef}]);
  my $row = $db->table ('table1')->find ({id => 123});

  ## Serialize
  $row->update ({value => \undef});
  my $value2 = $row->reload->get ('value');
  is $value2, undef;
} # _as_ref_undef_ref

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
