use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'hoge1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  $db->uuid_short (10)->then (sub {
    my $ids = shift;
    test {
      is 0+@$ids, 10;
      ok $ids->[0], $ids->[0];
      isnt $ids->[1], $ids->[0];
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
    } $c;
  });
} n => 3, name => 'uuid_short';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {ae => {dsn => $dsn, anyevent => 1, writable => 1}});

  $db->uuid_short (10, source_name => 'ae')->then (sub {
    my $ids = shift;
    test {
      is 0+@$ids, 10;
      ok $ids->[0], $ids->[0];
      isnt $ids->[1], $ids->[0];
    } $c;
  })->catch (sub {
    my $e = $_[0];
    test {
      is $e, undef, $e;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    test {
      done $c;
      undef $c;
    } $c;
  });
} n => 3, name => 'uuid_short source_name';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  dies_here_ok {
    $db->uuid_short (0);
  };

  done $c;
} n => 1, name => 'uuid_short n=0';

RUN;

=head1 LICENSE

Copyright 2020 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
