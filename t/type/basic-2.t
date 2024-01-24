use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use lib glob path (__FILE__)->parent->parent->parent->child ('t_deps/modules/*/lib');
BEGIN { $Test::Dongry::OldSQLMode = 1 }
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'root';

# ------ null_filled ------

test {
  my $c = shift;

  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'null_filled'},
      _create => 'CREATE TABLE table1 (id INT, value BINARY(12))',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'abc'}]);

  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), 'abc';
  is $row->get_bare ('value'), "abc\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => 'xyz'});
  $row->reload;
  is $row->get ('value'), 'xyz';
  is $row->get_bare ('value'), "xyz\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => '1234567890123456'});
  $row->reload;
  is $row->get ('value'), '123456789012';
  is $row->get_bare ('value'), '123456789012';

  $row->update ({value => undef});
  $row->reload;
  is $row->get ('value'), undef;
  is $row->get_bare ('value'), undef;

  $row->update ({value => ''});
  $row->reload;
  is $row->get ('value'), '';
  is $row->get_bare ('value'), 
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => "\x00"});
  $row->reload;
  is $row->get ('value'), '';
  is $row->get_bare ('value'),
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => "ab\x00c"});
  $row->reload;
  is $row->get ('value'), "ab\x00c";
  is $row->get_bare ('value'), "ab\x00c\x00\x00\x00\x00\x00\x00\x00\x00";

  done $c;
} n => 14, name => '_null_filled';

# ------ text_null_filled ------

test {
  my $c = shift;

  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_null_filled'},
      _create => 'CREATE TABLE table1 (id INT, value BINARY(12))',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'abc'}]);

  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), 'abc';
  is $row->get_bare ('value'), "abc\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => 'xyz'});
  $row->reload;
  is $row->get ('value'), 'xyz';
  is $row->get_bare ('value'), "xyz\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => '1234567890123456'});
  $row->reload;
  is $row->get ('value'), '123456789012';
  is $row->get_bare ('value'), '123456789012';

  $row->update ({value => undef});
  $row->reload;
  is $row->get ('value'), undef;
  is $row->get_bare ('value'), undef;

  $row->update ({value => "\x{6000}\x{2000}\x{100}"});
  $row->reload;
  is $row->get ('value'), "\x{6000}\x{2000}\x{100}";
  is $row->get_bare ('value'),
      encode_web_utf8 "\x{6000}\x{2000}\x{100}\x00\x00\x00\x00";

  $row->update ({value => ''});
  $row->reload;
  is $row->get ('value'), '';
  is $row->get_bare ('value'), 
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => "\x00"});
  $row->reload;
  is $row->get ('value'), '';
  is $row->get_bare ('value'),
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

  $row->update ({value => "ab\x00c"});
  $row->reload;
  is $row->get ('value'), "ab\x00c";
  is $row->get_bare ('value'), "ab\x00c\x00\x00\x00\x00\x00\x00\x00\x00";

  done $c;
} n => 16, name => "_text_null_filled";

test {
  my $c = shift;

  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'set'},
      _create => 'CREATE TABLE table1 (id INT, value SET("a","b","c"))',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'a,b'}]);

  my $row = $db->table ('table1')->find ({id => 123});
  eq_or_diff $row->get ('value'), {a => 1, b => 1};
  is $row->get_bare ('value'), 'a,b';

  $row->update ({value => {a => 2, b => 0, c => 1, d => 4}});
  $row->reload;
  eq_or_diff $row->get ('value'), {a => 1, c => 1};
  like $row->get_bare ('value'), qr{^(?:a,c|c,a)$};

  done $c;
} n => 4, name => "_set";

RUN;

=head1 LICENSE

Copyright 2011-2024 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
