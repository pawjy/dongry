package test::Dongry::Type::direct;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Type;
use Dongry::Type::Time;

sub _parse : Test(2) {
  my $v1 = Dongry::Type->parse ('as_ref', 'hoge');
  is ref $v1, 'SCALAR';
  is $$v1, 'hoge';
} # _parse

sub _parse_undef : Test(1) {
  my $v1 = Dongry::Type->parse ('as_ref', undef);
  is $v1, undef;
} # _parse_undef

sub _parse_broken : Test(1) {
  my $v1 = Dongry::Type->parse ('timestamp', '2012-01');
  is $v1, undef;
} # _parse_broken

sub _parse_unknown : Test(1) {
  dies_ok {
    Dongry::Type->parse ('month_type_not_found', '2012-01');
  };
} # _parse_unknown

sub _serialize : Test(1) {
  my $v1 = Dongry::Type->serialize ('as_ref', \'hoge');
  is $v1, 'hoge';
} # _serialize

sub _serialize_bad : Test(1) {
  dies_ok {
    Dongry::Type->serialize ('as_ref', {hoge => 'abc'});
  };
} # _serialize_bad

sub _serialize_unknown : Test(1) {
  dies_ok {
    Dongry::Type->serialize ('my_struct', {hoge => 'abc'});
  };
} # _serialize_unknown

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
