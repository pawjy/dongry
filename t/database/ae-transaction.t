use strict;
use warnings;
use Path::Tiny;
use lib glob path (__FILE__)->parent->parent->parent->child ('t/lib');
use Test::Dongry;
use Dongry::Database;

my $dsn = test_dsn 'hoge1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1}});

  $db->transaction->then (sub {
    my $tr = $_[0];
    test {
      isa_ok $tr, 'Dongry::Database::AETransaction';
      is $tr->debug_info, '{DBTransaction: AE}';
    } $c;
    return $tr->commit;
  })->then (sub {
    return $db->execute ('show tables', undef, source_name => 'master');
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'transaction object, empty commit';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int)', {name => $name})->then (sub {
    return $db->transaction;
  })->then (sub {
    my $tr = $_[0];
    return Promise->resolve->then (sub {
      return $tr->insert ($name, [{id => 3}]);
    })->then (sub {
      return $tr->commit;
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master');
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 3;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'transaction insert, commit';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int)', {name => $name})->then (sub {
    return $db->transaction;
  })->then (sub {
    my $tr = $_[0];
    return Promise->resolve->then (sub {
      return $tr->insert ($name, [{id => 3}]);
    })->then (sub {
      return $tr->rollback;
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master');
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 0;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 1, name => 'transaction insert, rollback';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int)', {name => $name})->then (sub {
    return $db->transaction;
  })->then (sub {
    my $tr = $_[0];
    my $p = Promise->resolve->then (sub {
      return $tr->insert ($name, [{id => 3}]);
    })->then (sub {
      return $tr->commit;
    });
    return $db->update ($name, {id => 4}, where => {id => 3});
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'transaction and non-transaction inserts';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    my $p = $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->select ($name, {id => {'>=', 0}})->then (sub {
        return $tr->insert ($name, [{id => 3, value => $_[0]->row_count}]);
      })->then (sub {
        return $tr->commit;
      });
    });
    my $q = $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->select ($name, {id => {'>=', 0}})->then (sub {
        return $tr->insert ($name, [{id => 4, value => $_[0]->row_count}]);
      })->then (sub {
        return $tr->commit;
      });
    });
    return $q;
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 2;
      is $rows->[0]->{id}, 3;
      is $rows->[0]->{value}, 0;
      is $rows->[1]->{id}, 4;
      is $rows->[1]->{value}, 1;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 5, name => 'transactions 1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    my $p = $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->select ($name, {id => {'>=', 0}})->then (sub {
        return $tr->insert ($name, [{id => 3, value => $_[0]->row_count}]);
      })->then (sub {
        return $tr->rollback;
      });
    });
    my $q = $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->select ($name, {id => {'>=', 0}})->then (sub {
        return $tr->insert ($name, [{id => 4, value => $_[0]->row_count}]);
      })->then (sub {
        return $tr->commit;
      });
    });
    return $q;
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 0;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 3, name => 'transactions 2';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->execute ('update :table:id set value = 51 where id = 4', {table => $name})->then (sub {
        return $tr->commit;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 51;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 3, name => 'transaction execute commit';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->execute ('update :table:id set value = 51 where id = 4', {table => $name})->then (sub {
        return $tr->rollback;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 3, name => 'transaction execute rollback';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->update ($name, {value => 51}, where => {id => 4})->then (sub {
        return $tr->commit;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 51;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 3, name => 'transaction update commit';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->update ($name, {value => 51}, where => {id => 4})->then (sub {
        return $tr->rollback;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 3, name => 'transaction update rollback';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->delete ($name, {id => 4})->then (sub {
        return $tr->commit;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 0;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 1, name => 'transaction delete commit';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int)', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->delete ($name, {id => 4})->then (sub {
        return $tr->rollback;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 3, name => 'transaction delete rollback';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return $tr->insert ($name, [{id => 4, value => 10}])->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $err = $_[0];
        test {
          ok $err->is_error, $err; # duplicate entry
        } $c;
      })->then (sub {
        return $tr->insert ($name, [{id => 5, value => 20}]);
      })->then (sub {
        return $tr->commit;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 2;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
      is $rows->[1]->{id}, 5;
      is $rows->[1]->{value}, 20;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 6, name => 'error in transaction 1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return Promise->resolve->then (sub {
        return $tr->insert ($name, [{id => 5, value => 20}]);
      })->then (sub {
        return $tr->insert ($name, [{id => 4, value => 10}]);
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $err = $_[0];
        test {
          ok $err->is_error, $err; # duplicate entry
        } $c;
      });
    }); # $tr is discarded (implicit rollback)
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master', order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 4, name => 'error in transaction 2';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1},
                   default => {dsn => $dsn, anyevent => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return Promise->resolve->then (sub {
        return $tr->insert ($name, [{id => 5, value => 20}]);
      })->then (sub {
        (delete $db->{dbhs}->{master})->disconnect;
        return undef;
      })->then (sub {
        return $tr->insert ($name, [{id => 4, value => 10}]);
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $err = $_[0];
        test {
          ok $err->is_error, $err; # connection lost
        } $c;
      })->then (sub {
        return $tr->insert ($name, [{id => 6, value => 50}]);
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $err = $_[0];
        test {
          ok $err->is_error, $err; # connection lost
        } $c;
      })->then (sub {
        return $tr->commit;
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $err = $_[0];
        test {
          ok $err->is_error, $err; # connection lost
        } $c;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 6, name => 'connection is lost during transaction';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1},
                   default => {dsn => $dsn, anyevent => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    $db->transaction->then (sub {
      my $tr = $_[0];
      return Promise->resolve->then (sub {
        return $tr->insert ($name, [{id => 5, value => 20}]);
      })->then (sub {
        return $tr->commit;
      });
    });
    return $db->disconnect;
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 2;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
      is $rows->[1]->{id}, 5;
      is $rows->[1]->{value}, 20;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 5, name => 'disconnect scheduled while there is ongoing transaction';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1},
                   default => {dsn => $dsn, anyevent => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      return Promise->resolve->then (sub {
        return $tr->insert ($name, [{id => 5, value => 20}]);
      })->then (sub {
        return $tr->commit;
      })->then (sub {
        test {
          is $tr->debug_info, '{DBTransaction: AE, invalid}';
        } $c;
      })->then (sub {
        return $tr->select ($name, {id => 5});
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # $tr is invalid
        } $c;
      })->then (sub {
        return $tr->execute ('select uuid_short ()');
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # $tr is invalid
        } $c;
      })->then (sub {
        return $tr->insert ($name, [{id => 6, value => 30}]);
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # $tr is invalid
        } $c;
      })->then (sub {
        return $tr->update ($name, {value => 30}, where => {id => 4});
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # $tr is invalid
        } $c;
      })->then (sub {
        return $tr->delete ($name, {id => 5});
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # $tr is invalid
        } $c;
      })->then (sub {
        return $tr->commit;
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # $tr is invalid
        } $c;
      })->then (sub {
        return $tr->rollback;
      })->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # $tr is invalid
        } $c;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 2;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
      is $rows->[1]->{id}, 5;
      is $rows->[1]->{value}, 20;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 13, name => 'invalid object methods';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1},
                   default => {dsn => $dsn, anyevent => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->transaction->then (sub {
      my $tr = $_[0];
      $db->onconnect (sub { # this is never invoked
        return $tr->insert ($name, [{id => 5, value => 20}]);
      });
      return Promise->resolve->then (sub {
        return $tr->insert ($name, [{id => 6, value => 30}]);
      })->then (sub {
        return $tr->commit;
      });
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 2;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
      is $rows->[1]->{id}, 6;
      is $rows->[1]->{value}, 30;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 5, name => 'onconnect 1';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1},
                   default => {dsn => $dsn, anyevent => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    $db->onconnect (sub {
      return $db->transaction->then (sub {
        test { ok 0, 'transaction should reject' } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e;
        } $c;
      });
    });
    return $db->select ($name, {id => {'>=', 0}}, source_name => 'master'); # connect
  })->then (sub {
    $db->onconnect (undef);
    return $db->select ($name, {id => {'>', 0}}, order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 4, name => 'onconnect 2';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1},
                   default => {dsn => $dsn, anyevent => 1}});

  my $name = rand;
  my $tr;
  $db->execute ('create table :name:id (id int, value int, unique key (id))', {name => $name})->then (sub {
    return $db->insert ($name, [{id => 4, value => 5}]);
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    return $db->transaction->then (sub {
      $tr = $_[0];
      return $tr->insert ($name, [{id => 5, value => 10}]);
    })->then (sub {
      (delete $db->{dbhs}->{master})->disconnect; # connection is lost
      return undef;
    })->then (sub {
      return $tr->insert ($name, [{id => 8, value => 40}])->then (sub {
        test { ok 0 } $c;
      }, sub {
        my $e = $_[0];
        test {
          ok $e->is_error, $e; # connection is lost
        } $c;
      });
    });
  })->then (sub {
    ## This function will never invoked as the previous transaction's
    ## session in $db is not concluded until $tr is discarded.
    #my $tr2;
    #return $db->transaction->then (sub {
    #  $tr2 = $_[0];
    #  return $tr->insert ($name, [{id => 6, value => 20}])->then (sub {
    #    test { ok 0 } $c;
    #  }, sub {
    #    my $e = $_[0];
    #    test {
    #      ok $e->is_error, $e;
    #    } $c;
    #  });
    #})->then (sub {
    #  return $tr2->insert ($name, [{id => 7, value => 30}]);
    #})->then (sub {
    #  return $tr2->commit;
    #});
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, order => ['id', 'asc']);
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 4;
      is $rows->[0]->{value}, 5;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 4, name => 'reusing transaction after connection abort is not allowed';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int)', {name => $name})->then (sub {
    return $db->transaction;
  })->then (sub {
    my $tr = $_[0];
    return Promise->resolve->then (sub {
      return $tr->select ($name, {id => 3}, lock => 'update');
    })->then (sub {
      return $tr->insert ($name, [{id => 3}]);
    })->then (sub {
      return $tr->commit;
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master');
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 3;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'select lock=update';

test {
  my $c = shift;

  my $db = Dongry::Database->new
      (sources => {master => {dsn => $dsn, anyevent => 1, writable => 1}});

  my $name = rand;
  $db->execute ('create table :name:id (id int)', {name => $name})->then (sub {
    return $db->transaction;
  })->then (sub {
    my $tr = $_[0];
    return Promise->resolve->then (sub {
      return $tr->select ($name, {id => 3}, lock => 'share');
    })->then (sub {
      return $tr->insert ($name, [{id => 3}]);
    })->then (sub {
      return $tr->commit;
    });
  })->then (sub {
    return $db->select ($name, {id => {'>', 0}}, source_name => 'master');
  })->then (sub {
    my $got = $_[0];
    test {
      my $rows = $got->all;
      is 0+@$rows, 1;
      is $rows->[0]->{id}, 3;
    } $c;
  })->then (sub {
    return $db->disconnect;
  })->then (sub {
    done $c;
    undef $c;
  });
} n => 2, name => 'select lock=share';

RUN;

=head1 LICENSE

Copyright 2011-2018 Wakaba <wakaba@suikawiki.org>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
