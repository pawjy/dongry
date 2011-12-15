package Dongry::Table;
use strict;
use warnings;
our $VERSION = '1.0';
use Carp;
use List::Rubyish;

push our @CARP_NOT, qw(Dongry::Database);

sub new ($;%) {
  my $class = shift;
  return bless {@_}, $class;
} # new

sub name ($) {
  return $_[0]->{name};
} # name

sub schema ($) {
  my $schema = $_[0]->{db}->schema or return undef;
  return $schema->{$_[0]->{name}}; # or undef
} # schema

sub _serialize_values ($$) {
  my ($self, $values) = @_;
  my $schema = $self->schema;
  my $s_values = {};
  for my $name (keys %$values) {
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

sub insert ($$;%) {
  my ($self, $data, %args) = @_;
  my $s_data = [];
  for my $values (@$data) {
    my $schema = $self->schema || {};

    for (keys %{$schema->{default} or {}}) {
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
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  my $row = $self->insert ([$values], %args)->first_as_row;
  $row->{flags} = $args{flags} if $args{flags};
  return $row;
} # create

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

sub new_row ($%) {
  my $class = shift;
  return bless {@_}, $class . '::Row';
} # new

package Dongry::Table::Row;
our $VERSION = '1.0';
use Carp;

our $CARP_NOT = qw(Dongry::Table);

sub table_name ($) {
  return $_[0]->{table_name};
} # table_name

sub table_schema ($) {
  my $schema = $_[0]->{db}->schema or return undef;
  return $schema->{$_[0]->{table_name}}; # or undef
} # table_schema

sub get ($$) {
  my ($self, $name) = @_;
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
  return $_[0]->{data}->{$_[1]};
} # get_bare

sub primary_key_values ($) {
  my $self = shift;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;

  my $schema = $self->table_schema or croak "No schema";
  my $pk = $schema->{primary_keys};
  croak "No primary key" if not $pk or not @$pk;
  my $data = $self->{data};
  return {map {
    croak "Primary key |$_| has no value" unless defined $data->{$_};
    ($_ => $data->{$_});
  } @$pk};
} # primary_key_values

sub set ($$;%) {
  my ($self, $values, %args) = @_;
  #local $Carp::CarpLevel = $Carp::CarpLevel + 1;
  croak "No value to set" unless keys %$values;

  my $pk_values = $self->primary_key_values;
  for (keys %$pk_values) {
    croak "Cannot modify primary key column |$_|" if exists $values->{$_};
  }

  my $schema = $self->table_schema || {};
  my $s_values = {};
  for my $name (keys %$values) {
    my $type = $schema->{type}->{$name};
    if ($type) {
      my $handler = $Dongry::Types->{$type}
          or croak "Type handler for |$type| is not defined";
      $s_values->{$name} = $handler->{serialize}->($values->{$name});
    } else {
      $s_values->{$name} = $values->{$name};
    }
  }

  my $result = $self->{db}->update
      ($self->table_name, $s_values, $pk_values);
  croak "@{[$result->{row_count}]} rows are modified by an update"
      unless $result->{row_count} == 1;

  for (keys %$values) {
    $self->{data}->{$_} = $s_values->{$_};
    $self->{parsed_data}->{$_} = $values->{$_};
  }
} # set

sub flags ($) {
  return $_[0]->{flags} ||= {};
} # flags

sub reload ($;%) {
  my ($self, %args) = @_;
  my $pk_values = $self->primary_key_values;
  return $self->{db}->select ($self->table_name, $pk_values)->first_as_row;
} # reload

1;

=head1 LICENSE

Copyright 2011 Wakaba <w@suika.fam.cx>.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
