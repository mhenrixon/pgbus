# frozen_string_literal: true

module Pgbus
  class BaseModel < ActiveRecord::Base
    self.abstract_class = true
  end
end
