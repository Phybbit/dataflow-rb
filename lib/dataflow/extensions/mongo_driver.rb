# frozen_string_literal: true
module Mongo
  class Collection
    class View
      attr_reader :cursor

      def initial_query
        @cursor = nil
        result = nil

        read_with_retry do
          server = read.select_server(cluster, false)
          result = send_initial_query(server)
          @cursor = Cursor.new(view, result, server)
        end

        result
      end
    end
  end
end
