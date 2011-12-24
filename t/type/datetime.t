package test::Dongry::Type::DateTime;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use DateTime;
use Dongry::Database;
use Dongry::Type::DateTime;

sub _version : Test(1) {
  ok $Dongry::Type::DateTime::VERSION;
} # _version

# ------ timestamp_as_DateTime ------

sub _datetime_valid : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T10:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'UTC');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-03T01:16:21';
  is $row->get_bare ('value'), '2001-12-03 01:16:21';
} # _datetime_valid

sub _datetime_valid_floating : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T10:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'floating');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-03T01:16:21';
  is $row->get_bare ('value'), '2001-12-03 01:16:21';
} # _datetime_valid_floating

sub _datetime_valid_jst : Test(7) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T10:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'Asia/Tokyo');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-02T16:16:21';
  is $row->get_bare ('value'), '2001-12-02 16:16:21';
  is $value1->time_zone->name, 'Asia/Tokyo';
  is_datetime $value1, '2001-12-02T16:16:21';
} # _datetime_valid_jst

sub _datetime_valid_est : Test(7) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T10:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'America/New_York');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-03T06:16:21';
  is $row->get_bare ('value'), '2001-12-03 06:16:21';
  is $value1->time_zone->name, 'America/New_York';
  is_datetime $value1, '2001-12-03T06:16:21';
} # _datetime_valid_est

sub _datetime_undef_zero : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '0000-00-00 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  $row->update ({value => undef});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), '0000-00-00 00:00:00';
} # _datetime_undef_zero

sub _datetime_undef_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => undef}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  $row->update ({value => undef});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), '0000-00-00 00:00:00';
} # _datetime_undef_undef

sub _datetime_bad_date : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2001-15-01 00:00:12'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_ok {
    $row->update ({value => '2001-10-61 00:12:51'});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), '2001-15-01 00:00:12';
} # _datetime_bad_date

sub _datetime_broken : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'abcde'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_ok {
    $row->update ({value => 'xyzzy'});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), 'abcde';
} # _datetime_broken

# ------ timestamp_jst_as_timestamp_jst ------

sub _timestamp_jst_valid_utc : Test(7) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T01:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'UTC');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-03T01:16:21';
  is $row->get_bare ('value'), '2001-12-03 10:16:21';
  is_datetime $value1, '2001-12-03T01:16:21';
  is $value1->time_zone->name, 'UTC';
} # _timestamp_jst_valid_utc

sub _timestamp_jst_valid_floating : Test(7) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T01:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'floating');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-03T01:16:21';
  is $row->get_bare ('value'), '2001-12-03 10:16:21';
  is_datetime $value1, '2001-12-03T01:16:21';
  is $value1->time_zone->name, 'floating';
} # _timestamp_jst_valid_floating

sub _timestamp_jst_valid_jst : Test(7) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T01:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'Asia/Tokyo');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-02T16:16:21';
  is $row->get_bare ('value'), '2001-12-03 01:16:21';
  is $value1->time_zone->name, 'Asia/Tokyo';
  is_datetime $value1, '2001-12-02T16:16:21';
} # _timestamp_jst_valid_jst

sub _timestamp_jst_valid_est : Test(7) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  isa_ok $value, 'DateTime';
  is_datetime $value, '2012-02-12T01:12:51';
  is $value->time_zone->name, 'UTC';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'America/New_York');
  $row->update ({value => $value1});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime $value2, '2001-12-03T06:16:21';
  is $row->get_bare ('value'), '2001-12-03 15:16:21';
  is $value1->time_zone->name, 'America/New_York';
  is_datetime $value1, '2001-12-03T06:16:21';
} # _timestamp_jst_valid_est

sub _timestamp_jst_undef_zero : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '0000-00-00 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  $row->update ({value => undef});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), '0000-00-00 00:00:00';
} # _timestamp_jst_undef_zero

sub _timestamp_jst_undef_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => undef}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  $row->update ({value => undef});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), '0000-00-00 00:00:00';
} # _timestamp_jst_undef_undef

sub _timestamp_jst_bad_date : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2001-15-01 00:00:12'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_ok {
    $row->update ({value => '2001-10-61 00:12:51'});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), '2001-15-01 00:00:12';
} # _timestamp_jst_bad_date

sub _timestamp_jst_broken : Test(5) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst_as_DateTime'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'abcde'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  local $DBIx::ShowSQL::COUNT = 1;
  local $DBIx::ShowSQL::SQLCount = 0;
  dies_ok {
    $row->update ({value => 'xyzzy'});
  };
  is $DBIx::ShowSQL::SQLCount, 0;
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), 'abcde';
} # _timestamp_jst_broken

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
