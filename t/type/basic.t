package test::Dongry::Type::basic;
use strict;
use warnings;
no warnings 'utf8';
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

# ------ as_ref ------

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

sub _as_ref_empty_ref : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => ''}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $$value, '';

  ## Serialize
  $row->update ({value => \''});
  my $value2 = $row->reload->get ('value');
  is $$value2, '';
} # _as_ref_empty_ref

sub _as_ref_zero_ref : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => 0}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $$value, 0;

  ## Serialize
  $row->update ({value => \0});
  my $value2 = $row->reload->get ('value');
  is $$value2, 0;
} # _as_ref_zero_ref

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

sub _as_ref_not_reef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '0'}]);
  my $row = $db->table ('table1')->find ({id => 123});

  ## Serialize
  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_ok {
    $row->update ({value => "abc"});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
} # _as_ref_not_ref

# ------ text ------

sub _text : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "abc \x{4000}\x{125}"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{4000}\x{125}";

  ## Serialize
  $row->update ({value => "\x{650}axy\x{1235}\x{F0124}"});
  my $value2 = $row->reload->get ('value');
  is $value2, "\x{650}axy\x{1235}\x{F0124}";
  is $row->get_bare ('value'), encode 'utf-8', "\x{650}axy\x{1235}\x{F0124}";
} # _text

sub _text_latin1 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => encode 'utf-8', "abc \xDE\xAC\x82"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \xDE\xAC\x82";

  ## Serialize
  $row->update ({value => "\xFC\xA4ab\xC4\x81"});
  my $value2 = $row->reload->get ('value');
  is $value2, "\xFC\xA4ab\xC4\x81";
  is $row->get_bare ('value'), encode 'utf-8', "\xFC\xA4ab\xC4\x81";
} # _text_latin1

sub _text_broken : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "abc \x9E\x82\xACa"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{FFFD}\x{FFFD}\x{FFFD}a";
} # _text_broken

sub _text_broken_2 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \x{FFFF}a"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{FFFD}a";

  ## Serialize
  $row->update ({value => "ab\x{FFFF}c"});
  is $row->reload->get ('value'), "ab\x{FFFD}c";
  is $row->get_bare ('value'), encode 'utf-8', "ab\x{FFFD}c";
} # _text_broken_2

sub _text_broken_3 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \x{D800}a"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{FFFD}a";

  ## Serialize
  $row->update ({value => "ab\x{D800}c"});
  is $row->reload->get ('value'), "ab\x{FFFD}c";
  is $row->get_bare ('value'), encode 'utf-8', "ab\x{FFFD}c";
} # _text_broken_3

sub _text_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => undef}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), undef;

  ## Serialize
  $row->update ({value => undef});
  my $value2 = $row->reload->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), undef;
} # _text_undef

sub _text_empty : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => ''}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), '';

  ## Serialize
  $row->update ({value => ''});
  my $value2 = $row->reload->get ('value');
  is $value2, '';
  is $row->get_bare ('value'), '';
} # _text_empty

sub _text_zero : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '0'}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), '0';

  ## Serialize
  $row->update ({value => '0'});
  my $value2 = $row->reload->get ('value');
  is $value2, '0';
  is $row->get_bare ('value'), '0';
} # _text_zero

# ------ text_as_ref ------

sub _text_as_ref : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "abc \x{4000}\x{125}"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is ref $value, 'SCALAR';
  is $$value, "abc \x{4000}\x{125}";

  ## Serialize
  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  $row->update ({value => \"\x{650}axy\x{1235}\x{F0124}"});
  is $DBIx::ShowSQL::SQLCount, 1;
  my $value2 = $row->reload->get ('value');
  is $$value2, "\x{650}axy\x{1235}\x{F0124}";
  is $row->get_bare ('value'), encode 'utf-8', "\x{650}axy\x{1235}\x{F0124}";
} # _text_as_ref

sub _text_as_ref_latin1 : Test(4) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => encode 'utf-8', "abc \xDE\xAC\x82"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \xDE\xAC\x82";

  ## Serialize
  $row->update ({value => \"\xFC\xA4ab\xC4\x81"});
  my $value2 = $row->reload->get ('value');
  is $$value2, "\xFC\xA4ab\xC4\x81";
  is $row->get_bare ('value'), encode 'utf-8', "\xFC\xA4ab\xC4\x81";
} # _text_as_ref_latin1

sub _text_as_ref_broken : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "abc \x9E\x82\xACa"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \x{FFFD}\x{FFFD}\x{FFFD}a";
} # _text_as_ref_broken

sub _text_as_ref_broken_2 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \x{FFFF}a"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \x{FFFD}a";

  ## Serialize
  $row->update ({value => \"ab\x{FFFF}c"});
  is ${$row->reload->get ('value')}, "ab\x{FFFD}c";
  is $row->get_bare ('value'), encode 'utf-8', "ab\x{FFFD}c";
} # _text_as_ref_broken_2

sub _text_as_ref_broken_3 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \x{D800}a"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \x{FFFD}a";

  ## Serialize
  $row->update ({value => \"ab\x{D800}c"});
  is ${$row->reload->get ('value')}, "ab\x{FFFD}c";
  is $row->get_bare ('value'), encode 'utf-8', "ab\x{FFFD}c";
} # _text_as_ref_broken_3

sub _text_as_ref_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => undef}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), undef;

  ## Serialize
  $row->update ({value => undef});
  my $value2 = $row->reload->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), undef;
} # _text_as_ref_undef

sub _text_as_ref_undef_ref : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => undef}]);
  my $row = $db->table ('table1')->find ({id => 123});

  ## Serialize
  $row->update ({value => \undef});
  my $value2 = $row->reload->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), undef;
} # _text_as_ref_undef_ref

sub _text_as_ref_empty_ref : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => ''}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, '';

  ## Serialize
  $row->update ({value => \''});
  my $value2 = $row->reload->get ('value');
  is $$value2, '';
  is $row->get_bare ('value'), '';
} # _text_as_ref_empty_ref

sub _text_as_ref_zero_ref : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '0'}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, '0';

  ## Serialize
  $row->update ({value => \'0'});
  my $value2 = $row->reload->get ('value');
  is $$value2, '0';
  is $row->get_bare ('value'), '0';
} # _text_as_ref_zero_ref

sub _text_as_ref_not_reef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '0'}]);
  my $row = $db->table ('table1')->find ({id => 123});

  ## Serialize
  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_ok {
    $row->update ({value => "abc"});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
} # _text_as_ref_not_ref

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
