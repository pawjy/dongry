package Dongry::Query;
use strict;
use warnings;
our $VERSION = '2.0';

push our @CARP_NOT, qw(Dongry::Database);

sub new ($%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub new_null_query ($) {
  return bless {}, $_[0];
} # new_null_query

sub clone ($) {
  my $class = ref $_[0];
  return bless {%{$_[0]}}, $class;
} # clone

sub is_null ($) {
  return not defined $_[0]->{table_name};
} # is_null

sub db { if (@_ > 1) { $_[0]->{db} = $_[1] } return $_[0]->{db} }

sub table_name {
  if (@_ > 1) { $_[0]->{table_name} = $_[1] } return $_[0]->{table_name};
} # table_name

sub table {
  return $_[0]->{table} ||= $_[0]->db->table ($_[0]->table_name);
} # table

sub distinct ($;$) {
  if (@_ > 1) { $_[0]->{distinct} = $_[1] } return $_[0]->{distinct};
} # distinct

sub fields {
  if (@_ > 1) { $_[0]->{fields} = $_[1] } return $_[0]->{fields};
} # fields

sub where {
  if (@_ > 1) { $_[0]->{where} = $_[1] } return $_[0]->{where};
} # where

sub order {
  if (@_ > 1) { $_[0]->{order} = $_[1] } return $_[0]->{order};
} # order

sub group {
  if (@_ > 1) { $_[0]->{group} = $_[1] } return $_[0]->{group};
} # group

sub source_name {
  if (@_ > 1) { $_[0]->{source_name} = $_[1] } return $_[0]->{source_name};
} # source_name

sub lock {
  if (@_ > 1) { $_[0]->{lock} = $_[1] } return $_[0]->{lock};
} # lock

sub item_list_filter {
  my $self = $_[0];
  if ($self->{item_list_filter}) {
    return scalar $self->{item_list_filter}->(@_);
  } else {
    return $_[1];
  }
} # item_list_filter

sub find_all {
  my ($self, %args) = @_;

  if ($self->is_null) {
    if ($args{cb}) {
      local $_ = $self->{db}->_list;
      require Dongry::Database;
      my $result = bless {}, 'Dongry::Database::Executed';
      local $Carp::CarpLevel = $Carp::CarpLevel + 1;
      $args{cb}->($self->{db}, $result);
      return $_;
    } else {
      return $self->{db}->_list;
    }
  }

  my %param = (distinct => $self->distinct,
               fields => $self->fields,
               and_where => $args{and_where},
               group => $self->group,
               order => $self->order,
               offset => $args{offset},
               limit => $args{limit},
               source_name => $args{source_name} || $self->source_name,
               lock => $args{lock} || $self->lock);

  if (my $cb = $args{cb}) {
    my $return;
    $param{cb} = sub {
      if ($_[1]->is_success) {
        local $_ = $return = $self->item_list_filter ($_);
        $cb->(@_);
      } else {
        local $_ = $return = undef;
        $cb->(@_);
      }
    };
    if (defined wantarray) {
      my $dummy = $self->table->find_all ($self->where, %param);
      return $return;
    } else {
      $self->table->find_all ($self->where, %param);
    }
  } else {
    return $self->item_list_filter
        ($self->table->find_all ($self->where, %param));
  }
} # find_all

sub find {
  my ($self, %args) = @_;

  if ($self->is_null) {
    if ($args{cb}) {
      local $_ = undef;
      require Dongry::Database;
      my $result = bless {}, 'Dongry::Database::Executed';
      local $Carp::CarpLevel = $Carp::CarpLevel + 1;
      $args{cb}->($self->{db}, $result);
      return $_;
    } else {
      return undef;
    }
  }

  my %param = (fields => $self->fields,
               and_where => $args{and_where},
               group => $self->group,
               order => $self->order,
               offset => 0,
               limit => 1,
               source_name => $args{source_name} || $self->source_name,
               lock => $args{lock} || $self->lock);

  if (my $cb = $args{cb}) {
    my $return;
    $param{cb} = sub {
      if ($_[1]->is_success) {
        local $_ = $return = $self->item_list_filter ($_)->[0]; # or undef
        $cb->(@_);
      } else {
        local $_ = $return = undef;
        $cb->(@_);
      }
    };
    if (defined wantarray) {
      my $dummy = $self->table->find_all ($self->where, %param);
      return $return;
    } else {
      $self->table->find_all ($self->where, %param);
    }
  } else {
    return $self->item_list_filter
        ($self->table->find_all ($self->where, %param))->[0]; # undef
  }
} # find

sub count {
  my ($self, %args) = @_;

  if ($self->is_null) {
    if ($args{cb}) {
      local $_ = 0;
      require Dongry::Database;
      my $result = bless {}, 'Dongry::Database::Executed';
      local $Carp::CarpLevel = $Carp::CarpLevel + 1;
      $args{cb}->($self->{db}, $result);
      return $_;
    } else {
      return 0;
    }
  }

  my %param = (
    and_where => $args{and_where},
    source_name => $args{source_name} || $self->source_name,
    lock => $args{lock} || $self->lock,
  );

  my $group = $self->group;
  if ($group) {
    ## How |$self->group| and |$self->distinct| should interact is
    ## unclear...
    $param{fields} = {-count => $group, distinct => 1, as => 'count'};
  } else {
    if ($self->distinct) {
      $param{fields} = {-count => $self->fields, distinct => 1, as => 'count'};
    } else {
      $param{fields} = {-count => undef, as => 'count'};
    }
  }

  if (my $cb = $args{cb}) {
    $param{cb} = sub {
      local $_ = $_[1]->is_success ? ($_ ? $_->get ('count') : 0) : undef;
      $cb->(@_);
    };
  }

  if (defined wantarray) {
    my $row = $self->table->find ($self->where, %param);
    return $row ? $row->get ('count') : 0;
  } else {
    $self->table->find ($self->where, %param);
  }
} # count

sub debug_info ($) {
  my $self = shift;
  return sprintf '{Query: %s}',
      $self->is_null ? '(null)' : $self->table_name;
} # debug_info

sub DESTROY {
  if ($Dongry::LeakTest) {
    warn "Possible memory leak by object " . ref $_[0];
  }
} # DESTROY

1;

=head1 LICENSE

Copyright 2011-2012 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
