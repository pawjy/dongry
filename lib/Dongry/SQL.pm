package Dongry::SQL;
use strict;
use warnings;
our $VERSION = '1.0';
use Carp;
use Exporter::Lite;

our @EXPORT;

our $SortKeys;

## <http://dev.mysql.com/doc/refman/5.6/en/identifiers.html>.
push @EXPORT, qw(quote);
sub quote ($) {
  my $s = $_[0];
  $s =~ s/`/``/g;
  return q<`> . $s . q<`>;
} # quote

## <http://dev.mysql.com/doc/refman/5.6/en/string-literals.html>.
push @EXPORT, qw(like);
sub like ($) {
  my $s = $_[0];
  $s =~ s/([\\%_])/\\$1/g;
  return $s;
} # like

push @EXPORT, qw(fields);
sub fields ($);
sub fields ($) {
  if (not defined $_[0]) {
    return '*';
  } elsif (not ref $_[0]) {
    return quote $_[0];
  } elsif (ref $_[0] eq 'ARRAY') {
    if (@{$_[0]}) {
      return join ', ', map { fields ($_) } @{$_[0]};
    } else {
      croak 'Array reference cannot be empty';
    }
  } elsif (ref $_[0] eq 'HASH') {
    my $func = [grep { /^-/ } keys %{$_[0]}]->[0] || '';
    if ($func =~ /\A-(count|min|max|sum)\z/) {
      my $v = (uc $1) . '(';
      $v .= 'DISTINCT ' if $_[0]->{distinct};
      $v .= fields ($_[0]->{$func});
      $v .= ')';
      $v .= ' AS ' . quote $_[0]->{as} if defined $_[0]->{as};
      return $v;
    } else {
      if ($func) {
        croak sprintf 'Field function %s is not supported', $func;
      } else {
        croak 'Hash reference must contain a field function name';
      }
    }
  } elsif (ref $_[0] eq 'Dongry::SQL::BareFragment') {
    return ${$_[0]};
  } else {
    croak sprintf 'Field value %s is not supported', $_[0];
  }
} # fields

push @EXPORT, qw(where);
sub where ($;$) {
  my ($values, $table_schema) = @_;

  if (ref $values eq 'HASH') {
    my @and;
    my @placeholder;
    for my $key (keys %$values) {
      my $sql = quote $key;
      if (not defined $values->{$key}) {
        $sql .= ' IS NULL';
      } elsif (ref $values->{$key} eq 'HASH') {
        my $type = [grep { /^[-<>!=]/ } keys %{$values->{$key}}]->[0];
        my $op = {
            -eq => '=',  '==' => '=',
            -ne => '!=', '!=' => '!=', -not => '!=',
            -lt => '<',  '<'  => '<',
            -le => '<=', '<=' => '<=',
            -gt => '>',  '>'  => '>',
            -ge => '>=', '>=' => '>=',
            -like => 'LIKE',
            -prefix => 'LIKE', -infix => 'LIKE', -suffix => 'LIKE',
            -regexp => 'REGEXP',
            -in => '-in',
        }->{$type || ''} || '';
        if (not $op) {
          if (defined $type) {
            croak "Unknown operator |$type|";
          } else {
            croak "No operator specified";
          }
        } elsif (not defined $values->{$key}->{$type}) {
          if ($op eq '=') {
            $sql .= ' IS NULL';
          } elsif ($op eq '!=') {
            $sql .= ' IS NOT NULL';
          } else {
            croak "Operator |$type| does not allow an undef value";
          }
        } elsif ($op eq '-in') {
          my $list = $values->{$key}->{$type};
          croak "List for |-in| is empty" unless @$list;
          $sql .= ' IN (' . (join ', ', ('?') x @$list) . ')';
          push @placeholder, grep {
            croak "An undef is found in |-in| list" if not defined $_;
            croak "A reference is found in |-in| list" if ref $_;
            1;
          } @$list;
        } elsif (ref $values->{$key}->{$type}) {
          croak "A reference is specified for |$key|";
        } else {
          $sql .= ' ' . $op . ' ?';
          if ($type eq '-prefix') {
            push @placeholder, like ($values->{$key}->{$type}) . '%';
          } elsif ($type eq '-suffix') {
            push @placeholder, '%' . like ($values->{$key}->{$type});
          } elsif ($type eq '-infix') {
            push @placeholder, '%' . like ($values->{$key}->{$type}) . '%';
          } else {
            push @placeholder, $values->{$key}->{$type};
          }
        }
      } elsif (ref $values->{$key}) {
        croak "A reference is specified for |$key|";
      } else {
        $sql .= ' = ?';
        push @placeholder, $values->{$key};
      }
      push @and, $sql;
    }

    if (@and) {
      @and = sort { $a cmp $b } @and if $SortKeys;
      return ((join ' AND ', @and), \@placeholder);
    } else {
      croak "No condition is specified";
    }
  } elsif (ref $values eq 'ARRAY') {
    my ($sql, %bind) = @$values;
    croak 'No SQL template is specified'
        if not defined $sql or not length $sql;

    $sql =~ s{((`?)(\w+?)\2\s*(=|<=?|>=?|<>|!=|<=>)\s*)\?\s*}{$1:$3 }g;

    my %unused = map { $_ => 1 } keys %bind;
    my @placeholder;
    $sql =~ s{:(\w+)(?::(:?\w+))?}{
      my $key = $1;
      my $column = defined $2 ? $2 : $key;
      if (not defined $bind{$key}) {
        croak "Value for |$key| is not defined";
      }
      my $type = ref ($bind{$key});
      delete $unused{$key};
      if ($type eq 'ARRAY') {
        if (@{$bind{$key}}) {
          push @placeholder, grep { 
            croak "An undef is found in |$key| list" if not defined $_;
            croak "A reference is found in |$key| list" if ref $_;
            1;
          } @{$bind{$key}};
          join ', ', map { '?' } @{$bind{$key}};
        } else {
          croak "List for |$key| is empty";
        }
      } elsif ($type) {
        croak "A reference is specified for |$key|";
      } else {
        push @placeholder, $bind{$key};
        '?';
      }
    }eg;

    if (keys %unused) {
      croak sprintf "Values %s are not used",
          join ', ', map { "|$_|" } keys %unused;
    }

    return ($sql, \@placeholder);
  }

  # XXX

} # where

push @EXPORT, qw(order);
sub order ($$) {
  if (defined $_[1]) {
    my @s;
    for (0..(int (($#{$_[1]} + 2) / 2) - 1)) {
      push @s, (quote $_[1]->[$_ * 2]) . ' ' . (
        {
          'ASC' => 'ASC',
          'asc' => 'ASC',
          '1' => 'ASC',
          '+1' => 'ASC',
          'DESC' => 'DESC',
          'desc' => 'DESC',
          '-1' => 'DESC',
        }->{$_[1]->[$_ * 2 + 1] || 'ASC'} or
        (croak sprintf 'Unknown order: %s', $_[1]->[$_ * 2 + 1])
      );
    }
    return ' ORDER BY ' . join ', ', @s;
  } else {
    return '';
  }
} # order

# ------ Bare SQL fragment ------

package Dongry::SQL::BareFragment;
our $VERSION = '1.0';

1;
