package test::Dongry::SQL::basic;
use strict;
use warnings;
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::SQL;

sub _version : Test(2) {
  ok $Dongry::SQL::VERSION;
  ok $Dongry::SQL::BareFragment::VERSION;
} # _version

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
