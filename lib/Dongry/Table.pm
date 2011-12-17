package Dongry::Table;
use strict;
use warnings;
our $VERSION = '1.0';
use Carp;
use List::Rubyish;

push our @CARP_NOT, qw(Dongry::Database);

# ------------ Tables ------------

sub new ($;%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

# ------ Property accessors ------

sub name ($) {
  return $_[0]->{name};
} # name

sub schema ($) {
  my $schema = $_[0]->{db}->schema or return undef;
  return $schema->{$_[0]->{name}}; # or undef
} # schema

# ------ Serialization ------

sub _serialize_values ($$) {
  my ($self, $values) = @_;
  my $schema = $self->schema;
  my $s_values = {};
  for my $name (keys %$values) {
    if (defined $values->{$name} and
        ref $values->{$name} eq 'Dongry::Database::BareSQLFragment') {
      $s_values->{$name} = $values->{$name};
      next;
    }

    my $type = $schema->{type}->{$name};
    if ($type) {
      my $handler = $Dongry::Types->{$type}
          or croak "Type handler for |$type| is not defined";
      $s_values->{$name} = $handler->{serialize}->($values->{$name});
    } else {
      if (defined $values->{$name} and
          ref $values->{$name}) {
        croak "Type for |$name| is not defined but a reference is specified";
      } else {
        $s_values->{$name} = $values->{$name};
      }
    }
  }
  return $s_values;
} # _serialize_values

# ------ Insertion ------

sub insert ($$;%) {
  my ($self, $data, %args) = @_;

  my $s_data = [];
  for my $values (@$data) {
    my $schema = $self->schema || {};

    for (keys %{$schema->{default} or {}}) {
      next if defined $values->{$_};
      my $default = $schema->{default}->{$_};
      if (defined $default and ref $default eq 'CODE') {
        $values->{$_} = $default->();
      } else {
        $values->{$_} = $default;
      }
    }

    my $s_values = $self->_serialize_values ($values);
    push @$s_data, $s_values;
  }

  if ($args{duplicate} and ref $args{duplicate} eq 'HASH') {
    $args{duplicate} = $self->_serialize_values ($args{duplicate});
  }

  if (defined wantarray) {
    my $return = $self->{db}->insert ($self->name, $s_data, %args);
    $return->{parsed_data} = $data;
    return $return;
  } else {
    $self->{db}->insert ($self->name, $s_data, %args);
  }
} # insert

sub create ($$;%) {
  my ($self, $values, %args) = @_;
  my $row = $self->insert ([$values], %args)->first_as_row;
  $row->{flags} = $args{flags} if $args{flags};
  return $row;
} # create

# ------ Retrieval ------

# XXX tests

sub find ($$;%) {
  my ($self, $values, %args) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  my $s_values = $self->_serialize_values ($values);
  return $self->{db}
      ->select ($self->{name}, $s_values,
                limit => 1,
                source_name => $args{source_name},
                lock => $args{lock})
      ->first_as_row;
} # find

# XXX search

sub search_and_fill_as_row ($$$$$$;%) {
  my ($self,
      $list, $value_method_name => $column_name => $object_method_name,
      %args) = @_;
  
  my $ids = {map { $_->$value_method_name => 1 } @$list};
  my $map = {};
  $self->{db}->select
      ($self->{name},
       {$column_name => {in => [keys %$ids]}},
       source_name => $args{source_name},
       lock => $args{lock})
      ->for_each_as_row (sub {
    $map->{$_[0]->get ($column_name)} = $_[0];
  });
  $_->$object_method_name ($map->{$_->$value_method_name}) for @$list;
} # search_and_fill_as_row

sub search_and_fill_pair_as_row ($$$$$$$$;%) {
  my ($self,
      $list, $value_method_name1, $value_method_name2
      => $column_name1, $column_name2,
      => $object_method_name,
      %args) = @_;
  
  my $ids1 = {map { $_->$value_method_name1 => 1 } @$list};
  my $ids2 = {map { $_->$value_method_name2 => 1 } @$list};
  my $map = {};
  $self->{db}->select
      ($self->{name},
       {$column_name1 => {in => [keys %$ids1]},
        $column_name2 => {in => [keys %$ids2]}},
       source_name => $args{source_name},
       lock => $args{lock})
      ->for_each_as_row ($args{multiple} ? sub {
    ($map->{$_[0]->get ($column_name1)}
         ->{$_[0]->get ($column_name2)} ||= List::Rubyish->new)->push ($_[0]);
  } : sub {
    $map->{$_[0]->get ($column_name1)}->{$_[0]->get ($column_name2)} = $_[0];
  });
  my $default = $args{multiple} ? List::Rubyish->new : undef;
  $_->$object_method_name
      ($map->{$_->$value_method_name1}->{$_->$value_method_name2} || $default)
      for @$list;
} # search_and_fill_pair_as_row

# ------------ Table rows ------------

package Dongry::Table::Row;
our $VERSION = '1.0';
use Carp;

our $CARP_NOT = qw(Dongry::Table);

# ------ Property accessors ------

sub table_name ($) {
  return $_[0]->{table_name};
} # table_name

sub table_schema ($) {
  my $schema = $_[0]->{db}->schema or return undef;
  return $schema->{$_[0]->{table_name}}; # or undef
} # table_schema

sub flags ($) {
  return $_[0]->{flags} ||= {};
} # flags

# ------ Value accessors ------

sub get ($$) {
  my ($self, $name) = @_;
  croak "No data for column |$name|"
      if not exists $self->{data}->{$name} or
         ref $self->{data}->{$name} eq 'Dongry::Database::BareSQLFragment';
  return $self->{parsed_data}->{$name} if exists $self->{parsed_data}->{$name};

  my $schema = $self->table_schema || do {
    carp "No schema for table |$self->{table_name}|";
    +{};
  };
  my $type = $schema->{type}->{$name};
  if ($type) {
    my $handler = $Dongry::Types->{$type}
        or croak "Type handler for |$type| is not defined";
    return $self->{parsed_data}->{$name}
        = $handler->{parse}->($self->{data}->{$name});
  } else {
    return $self->{parsed_data}->{$name}
        = $self->{data}->{$name};
  }
} # get

sub get_bare ($$) {
  croak "No data for column |$_[1]|"
      if not exists $_[0]->{data}->{$_[1]} or
         ref $_[0]->{data}->{$_[1]} eq 'Dongry::Database::BareSQLFragment';
  return $_[0]->{data}->{$_[1]};
} # get_bare

sub primary_key_bare_values ($) {
  my $self = shift;
  my $schema = $self->table_schema || {};
  my $pk = $schema->{primary_keys};
  croak "No primary key" if not $pk or not @$pk;
  my $data = $self->{data};
  return {map {
    croak "Primary key |$_| has no value"
        if not defined $data->{$_} or
           ref $data->{$_} eq 'Dongry::Database::BareSQLFragment';
    ($_ => $data->{$_});
  } @$pk};
} # primary_key_bare_values

sub reload ($;%) {
  my ($self, %args) = @_;
  my $pk_values = $self->primary_key_bare_values;
  my $result = $self->{db}->select
      ($self->table_name, $pk_values, %args, limit => 2);
  if ($result->row_count != 1) {
    croak sprintf "There are %d rows for the primary keys", $result->row_count;
  }
  $self->{data} = $result->first;
  delete $self->{parsed_data};
  return $self;
} # reload

# ------ Modifications ------

sub set ($$;%) {
  my ($self, $values, %args) = @_;
  croak "No value to set" unless keys %$values;

  my $pk_values = $self->primary_key_bare_values;

  my $schema = $self->table_schema || {};
  my $s_values = {};
  for my $name (keys %$values) {
    if (defined $values->{$name} and
        ref $values->{$name} eq 'Dongry::Database::BareSQLFragment') {
      $s_values->{$name} = $values->{$name};
      next;
    }

    my $type = $schema->{type}->{$name};
    if ($type) {
      my $handler = $Dongry::Types->{$type}
          or croak "Type handler for |$type| is not defined";
      $s_values->{$name} = $handler->{serialize}->($values->{$name});
    } else {
      if (defined $values->{$name} and ref $values->{$name}) {
        croak "Type for |$name| is not defined but a reference is specified";
      } else {
        $s_values->{$name} = $values->{$name};
      }
    }
  }

  my $result = $self->{db}->update
      ($self->table_name, $s_values, $pk_values,
       source_name => $args{source_name});
  croak "@{[$result->{row_count}]} rows are modified by an update"
      unless $result->{row_count} == 1;

  for (keys %$values) {
    $self->{data}->{$_} = $s_values->{$_};
    $self->{parsed_data}->{$_} = $values->{$_};
  }
} # set

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
