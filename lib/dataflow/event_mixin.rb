# frozen_string_literal: true
module Dataflow
  module EventMixin
    extend ActiveSupport::Concern

    module ClassMethods
      def event(event_name)
        # re-open the base class
        handlers_var_name = "@#{event_name}_handlers"

        # Defines a class method called "event_name".
        # It will serve a global-level evt handler for this class.
        # @yield (optional) the event handler to add
        # @return Array the list of event handlers
        define_singleton_method(event_name) do |&block|
          handlers = instance_variable_get(handlers_var_name)

          unless handlers
            handlers = []
            instance_variable_set(handlers_var_name, [])
          end

          if block.present?
            handlers << block
            instance_variable_set(handlers_var_name, handlers)
          end

          # return all events from the hierarchy
          superclass_handlers = []
          superclass = self.superclass
          while superclass
            superclass_handlers += superclass.instance_variable_get(
              :"@#{event_name}_handlers"
            ) || []
            superclass = superclass.superclass
          end

          handlers + superclass_handlers
        end

        # Defines a method called "event_name".
        # It will serve a instance-level evt handler.
        # @yield (optional) the event handler to add
        # @return Array the list of event handlers
        define_method(event_name) do |&block|
          handlers = instance_variable_get(handlers_var_name)

          unless handlers
            handlers = []
            instance_variable_set(handlers_var_name, [])
          end

          if block.present?
            handlers << block
            instance_variable_set(handlers_var_name, handlers)
          end

          handlers
        end

        # Defines a way to fire the event: "on_event_name(evt)"
        # @param *args a variable list of arguments passed to the handlers
        define_method("on_#{event_name}") do |*args|
          handlers = send(event_name) + self.class.send(event_name)
          handlers.each do |handler|
            begin
              handler.call(self, *args)
            rescue StandardError => e
              @logger&.log("ERROR IN HANDLER [on_#{event_name}]: #{e}")
              # ignore error in handlers
            end
          end
        end
      end
    end
  end
end
