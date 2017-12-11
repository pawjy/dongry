use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/lib');
use StandaloneTests;
use Dongry::SQL ();

test {
  my $c = shift;
  ok $Dongry::SQL::VERSION;
  ok $Dongry::SQL::BareFragment::VERSION;
  done $c;
} n => 2, name => 'version';

RUN;

=head1 LICENSE

Copyright 2011-2017 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
