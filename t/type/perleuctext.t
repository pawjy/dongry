package test::Dongry::Type::PerlEUCText;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use Dongry::Database;
use Dongry::Type::PerlEUCText;
use Encode;

sub _version : Test(1) {
  ok $Dongry::Type::PerlEUCText::VERSION;
} # _version

# ------ perl_euc_text ------

sub _perl_euc_text : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1',
       [{id => 123, value => encode 'euc-jp', "abc \x{4E00}\x{5025}"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{4E00}\x{5025}";

  ## Serialize
  $row->update ({value => "\x{6500}axy\x{4e35}\x{F0124}"});
  my $value2 = $row->reload->get ('value');
  is $value2, "\x{6500}axy\x{4e35}\x{3F}";
  is $row->get_bare ('value'), encode 'euc-jp', "\x{6500}axy\x{4E35}\x{3F}";
} # _perl_euc_text

sub _perl_euc_text_latin1 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => encode 'euc-jp', "abc \xA7\xB6\x82"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \xA7\xB6\x3F";

  ## Serialize
  $row->update ({value => "\xB6\xA7ab\xC4\x81"});
  my $value2 = $row->reload->get ('value');
  is $value2, "\xB6\xA7ab\xC4\x3F";
  is $row->get_bare ('value'), encode 'euc-jp', "\xB6\xA7ab\xC4\x3F";
} # _perl_euc_text_latin1

sub _perl_euc_text_broken : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "abc \x9E\x82\xACa"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{FFFD}\x{FFFD}\x{FFFD}a";
} # _perl_euc_text_broken

sub _perl_euc_text_broken_2 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \xFE\xFEa"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{FFFD}\x{FFFD}a";

  ## Serialize
  $row->update ({value => "ab\x{FFFF}c"});
  is $row->reload->get ('value'), "ab\x{003F}c";
  is $row->get_bare ('value'), encode 'euc-jp', "ab\x{003F}c";
} # _perl_euc_text_broken_2

sub _perl_euc_text_broken_3 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \xD8\x00a"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is $row->get ('value'), "abc \x{FFFD}\x00a";

  ## Serialize
  $row->update ({value => "ab\x{D800}c"});
  is $row->reload->get ('value'), "ab\x{3F}c";
  is $row->get_bare ('value'), encode 'euc-jp', "ab\x{003F}c";
} # _perl_euc_text_broken_3

sub _perl_euc_text_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
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
} # _perl_euc_text_undef

sub _perl_euc_text_empty : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
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
} # _perl_euc_text_empty

sub _perl_euc_text_zero : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text'},
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
} # _perl_euc_text_zero

# ------ perl_euc_text_as_ref ------

sub _perl_euc_text_as_ref : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1',
       [{id => 123, value => encode 'euc-jp', "abc \x{4E00}\x{3002}"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is ref $value, 'SCALAR';
  is $$value, "abc \x{4E00}\x{3002}";

  ## Serialize
  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  $row->update ({value => \"\x{6500}axy\x{4E35}\x{F0124}"});
  is $DBIx::ShowSQL::SQLCount, 1;
  my $value2 = $row->reload->get ('value');
  is $$value2, "\x{6500}axy\x{4E35}\x{3F}";
  is $row->get_bare ('value'), encode 'euc-jp', "\x{6500}axy\x{4E35}\x{3F}";
} # _perl_euc_text_as_ref

sub _perl_euc_text_as_ref_latin1 : Test(4) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => encode 'euc-jp', "abc \xDE\xAC\x82"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \xDE\xAC\x3F";

  ## Serialize
  $row->update ({value => \"\xFC\xA4ab\xC4\x81"});
  my $value2 = $row->reload->get ('value');
  is $$value2, "\xFC\xA4ab\xC4\x3F";
  is $row->get_bare ('value'), encode 'euc-jp', "\xFC\xA4ab\xC4\x3F";
} # _perl_euc_text_as_ref_latin1

sub _perl_euc_text_as_ref_broken : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => "abc \x9E\x82\xACa"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \x{FFFD}\x{FFFD}\x{FFFD}a";
} # _perl_euc_text_as_ref_broken

sub _perl_euc_text_as_ref_broken_2 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \xFF\xFFa"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \x{FFFD}\x{FFFD}a";

  ## Serialize
  $row->update ({value => \"ab\x{FFFF}c"});
  is ${$row->reload->get ('value')}, "ab\x{3F}c";
  is $row->get_bare ('value'), encode 'euc-jp', "ab\x{003F}c";
} # _perl_euc_text_as_ref_broken_2

sub _perl_euc_text_as_ref_broken_3 : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert
      ('table1', [{id => 123, value => "abc \x{D800}a"}]);

  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  is ${$row->get ('value')}, "abc \x{FFFD}\x{FFFD}\x{FFFD}a";

  ## Serialize
  $row->update ({value => \"ab\x{D800}c"});
  is ${$row->reload->get ('value')}, "ab\x{003F}c";
  is $row->get_bare ('value'), encode 'euc-jp', "ab\x{003F}c";
} # _perl_euc_text_as_ref_broken_3

sub _perl_euc_text_as_ref_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
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
} # _perl_euc_text_as_ref_undef

sub _perl_euc_text_as_ref_undef_ref : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
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
} # _perl_euc_text_as_ref_undef_ref

sub _perl_euc_text_as_ref_empty_ref : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
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
} # _perl_euc_text_as_ref_empty_ref

sub _perl_euc_text_as_ref_zero_ref : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
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
} # _perl_euc_text_as_ref_zero_ref

sub _perl_euc_text_as_ref_not_ref : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'perl_euc_text_as_ref'},
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
} # _perl_euc_text_as_ref_not_ref

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
