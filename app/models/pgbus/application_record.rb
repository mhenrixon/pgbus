# frozen_string_literal: true

module Pgbus
  class ApplicationRecord < ActiveRecord::Base
    self.abstract_class = true
  end
end
