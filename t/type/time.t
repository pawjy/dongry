package test::Dongry::Type::Time;
use strict;
use warnings;
no warnings 'utf8';
use Path::Class;
use lib file (__FILE__)->dir->parent->subdir ('lib')->stringify;
use Test::Dongry;
use base qw(Test::Class);
use DateTime;
use Dongry::Database;
use Dongry::Type::Time;

sub _version : Test(1) {
  ok $Dongry::Type::Time::VERSION;
} # _version

sub dt ($) {
  return DateTime->from_epoch (epoch => $_[0]);
} # dt

# ------ timestamp ------

sub _timestamp_valid : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is_datetime dt $value, '2012-02-12T10:12:51';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'UTC');
  $row->update ({value => $value1->epoch});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime dt $value2, '2001-12-03T01:16:21';
  is $row->get_bare ('value'), '2001-12-03 01:16:21';
} # _timestamp_valid

sub _timestamp_epoch : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1970-01-01 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 0;

  $row->update ({value => 0});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 0;
  is $row->get_bare ('value'), '1970-01-01 00:00:00';
} # _timestamp_epoch

sub _timestamp_near_epoch : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1970-01-01 00:00:10'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 10;

  $row->update ({value => 10});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 10;
  is $row->get_bare ('value'), '1970-01-01 00:00:10';
} # _timestamp_near_epoch

sub _timestamp_negative : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1969-12-31 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, -86400;

  $row->update ({value => -86400});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, -86400;
  is $row->get_bare ('value'), '1969-12-31 00:00:00';
} # _timestamp_negative

sub _timestamp_large : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2069-12-31 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 3155673600;

  $row->update ({value => 3155673600});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 3155673600;
  is $row->get_bare ('value'), '2069-12-31 00:00:00';
} # _timestamp_large

sub _timestamp_not_number : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1999-12-31 00:00:00'}]);
  my $row = $db->table ('table1')->find ({id => 123});

  $row->update ({value => 'abc def'});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 0;
  is $row->get_bare ('value'), '1970-01-01 00:00:00';
} # _timestamp_not_number

sub _timestamp_undef_zero : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
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
} # _timestamp_undef_zero

sub _timestamp_undef_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
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
} # _timestamp_undef_undef

sub _timestamp_bad_date : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2001-15-01 00:00:12'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is_datetime dt $value,
      DateTime->new (year => 2001, month => 3, day => 1, second => 12);
} # _timestamp_bad_date

sub _timestamp_bad_date_2 : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2001-12-01 27:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is_datetime dt $value,
      DateTime->new (year => 2001, month => 12, day => 2, hour => 3);
} # _timestamp_bad_date_2

sub _timestamp_broken : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'abcde'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;
} # _timestamp_broken

# ------ timestamp_jst_as_timestamp_jst ------

sub _timestamp_jst_valid : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value TIMESTAMP)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12 10:12:51'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is_datetime dt $value, '2012-02-12T01:12:51';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'UTC');
  $row->update ({value => $value1->epoch});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime dt $value2, '2001-12-03T01:16:21';
  is $row->get_bare ('value'), '2001-12-03 10:16:21';
} # _timestamp_jst_valid

sub _timestamp_jst_epoch : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1970-01-01 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, -9*60*60;

  $row->update ({value => -9*60*60});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, -9*60*60;
  is $row->get_bare ('value'), '1970-01-01 00:00:00';
} # _timestamp_jst_epoch

sub _timestamp_jst_near_epoch : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1970-01-01 09:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 0;

  $row->update ({value => 0});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 0;
  is $row->get_bare ('value'), '1970-01-01 09:00:00';
} # _timestamp_jst_near_epoch

sub _timestamp_jst_negative : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1969-12-31 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, -118800;

  $row->update ({value => -86400});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, -86400;
  is $row->get_bare ('value'), '1969-12-31 09:00:00';
} # _timestamp_jst_negative

sub _timestamp_jst_large : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value DATETIME)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2069-12-31 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 3155641200;

  $row->update ({value => 3155673600});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 3155673600;
  is $row->get_bare ('value'), '2069-12-31 09:00:00';
} # _timestamp_jst_large

sub _timestamp_jst_not_number : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1999-12-31 00:00:00'}]);
  my $row = $db->table ('table1')->find ({id => 123});

  $row->update ({value => 'abc def'});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 0;
  is $row->get_bare ('value'), '1970-01-01 09:00:00';
} # _timestamp_jst_not_number

sub _timestamp_jst_undef_zero : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
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
      type => {value => 'timestamp_jst'},
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

sub _timestamp_jst_bad_date : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2001-15-01 00:00:12'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is dt $value,
      DateTime->new (year => 2001, month => 3, day => 1, second => 12,
                     time_zone => 'Asia/Tokyo');
} # _timestamp_jst_bad_date

sub _timestamp_jst_broken : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'timestamp_jst'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'abcde'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;
} # _timestamp_jst_broken

# ------ date ------

sub _date_valid : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value DATE)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2012-02-12'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is_datetime dt $value, '2012-02-12T00:00:00';

  my $value1 = DateTime->new (year => 2001, month => 12, day => 3,
                              hour => 1, minute => 16, second => 21,
                              time_zone => 'UTC');
  $row->update ({value => $value1->epoch});
  $row->reload;
  my $value2 = $row->get ('value');
  is_datetime dt $value2, '2001-12-03T00:00:00';
  is $row->get_bare ('value'), '2001-12-03';
} # _date_valid

sub _date_epoch : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value DATE)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1970-01-01'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 0;

  $row->update ({value => 0});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 0;
  is $row->get_bare ('value'), '1970-01-01';
} # _date_epoch

sub _date_near_epoch : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value DATE)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1970-01-02'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 86400;

  $row->update ({value => 86400});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 86400;
  is $row->get_bare ('value'), '1970-01-02';
} # _date_near_epoch

sub _date_negative : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value DATE)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1969-12-31'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, -86400;

  $row->update ({value => -86400});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, -86400;
  is $row->get_bare ('value'), '1969-12-31';
} # _date_negative

sub _date_large : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value DATE)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2069-12-31'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, 3155673600;

  $row->update ({value => 3155673600});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 3155673600;
  is $row->get_bare ('value'), '2069-12-31';
} # _date_large

sub _date_not_number : Test(2) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '1999-12-31'}]);
  my $row = $db->table ('table1')->find ({id => 123});

  $row->update ({value => 'abc def'});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, 0;
  is $row->get_bare ('value'), '1970-01-01';
} # _date_not_number

sub _date_undef_zero : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value DATE)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '0000-00-00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;

  $row->update ({value => undef});
  $row->reload;
  my $value2 = $row->get ('value');
  is $value2, undef;
  is $row->get_bare ('value'), '0000-00-00';
} # _date_undef_zero

sub _date_undef_undef : Test(3) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
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
  is $row->get_bare ('value'), '0000-00-00';
} # _date_undef_undef

sub _date_bad_date : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2001-15-01'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is_datetime dt $value, DateTime->new (year => 2001, month => 3, day => 1);
} # _date_bad_date

sub _date_broken : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => 'abcde'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;
} # _date_broken

sub _date_datetime : Test(1) {
  my $db = new_db schema => {
    table1 => {
      primary_keys => ['id'],
      type => {value => 'date'},
      _create => 'CREATE TABLE table1 (id INT, value BLOB)',
    },
  };
  $db->insert ('table1', [{id => 123, value => '2001-12-01 00:00:00'}]);
  
  ## Parse
  my $row = $db->table ('table1')->find ({id => 123});
  my $value = $row->get ('value');
  is $value, undef;
} # _date_datetime

__PACKAGE__->runtests;

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
