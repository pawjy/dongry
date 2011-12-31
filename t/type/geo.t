package test::Dongry::Type::Geo;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use DateTime;
use Dongry::Database;
use Dongry::Type::Geo;

sub _version : Test(1) {
  ok $Dongry::Type::Geo::VERSION;
} # _version

# ------ geometry ------

sub _geometry : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'geometry'},
      _create => 'CREATE TABLE table1 (id INT, value GEOMETRY) Engine=MyISAM',
    },
  };

  $db->table ('table1')->create
      ({id => 123, value => {-lat => 120.4012201,
                             -lon => -50.45330111111}});

  my $result = $db->execute
      ('select *, y(value) as lat, x(value) as lon from table1');
  my $values = $result->first;
  is $values->{lat}, 120.4012201;
  is $values->{lon}, -50.4533011111;

  my $row = $db->table ('table1')->find
      ({id => 123}, fields => [undef,
                               {-y => 'value', as => 'lat'},
                               {-x => 'value', as => 'lon'}]);
  is $row->get ('lat'), 120.4012201;
  is $row->get ('lon'), -50.4533011111;
  dies_here_ok {
    $row->get ('value');
  };
} # _geometry

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
