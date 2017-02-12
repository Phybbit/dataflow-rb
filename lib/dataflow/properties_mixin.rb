# frozen_string_literal: true
module Dataflow
  module PropertiesMixin
    extend ActiveSupport::Concern

    module ClassMethods
      # Override the mongoid `field` method to produce a list of
      # properties for each node.
      def field(name, opts = {})
        add_property(name, opts)
        # make sure we pass mongoid-only keys to the superclass
        opts.delete(:editable)
        opts.delete(:required_for_computing)
        opts.delete(:values)
        super
      end

      def add_property(name, opts)
        # skip properties that start by underscore
        return if name =~ /^_/
        @properties ||= {}
        @properties[name] ||= {}
        @properties[name].merge!(opts)
      end

      def properties
        @properties ||= {}
        @properties.merge(superclass.properties)
      rescue NoMethodError => e
        # handle cases where we're already on top of the hierarchy.
        @properties
      end
    end
  end
end
