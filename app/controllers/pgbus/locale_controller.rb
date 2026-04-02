# frozen_string_literal: true

module Pgbus
  class LocaleController < ApplicationController
    def update
      locale = params[:locale].to_s
      if available_locales.include?(locale.to_sym)
        cookies[:pgbus_locale] = { value: locale, expires: 1.year.from_now, path: "/" }
        I18n.locale = locale
      end

      redirect_back fallback_location: pgbus.root_path
    end
  end
end
