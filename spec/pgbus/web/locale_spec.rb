# frozen_string_literal: true

require "rails_helper"

RSpec.describe Pgbus::ApplicationController do
  # Pgbus ships these locale files
  let(:pgbus_shipped_locales) { %i[da de en es fi fr it ja nb nl pt sv] }

  describe "#available_locales" do
    subject(:available_locales) { controller.send(:available_locales) }

    let(:controller) do
      ctrl = described_class.allocate
      ctrl.instance_variable_set(:@available_locales, nil)
      ctrl
    end

    context "when host app enforces available_locales (default Rails behavior)" do
      before do
        allow(I18n).to receive(:available_locales).and_return(%i[en de])
      end

      it "only includes locales available in both pgbus and the host app" do
        expect(available_locales).to contain_exactly(:en, :de)
      end

      it "excludes pgbus locales not available in the host app" do
        expect(available_locales).not_to include(:pt, :fr, :ja)
      end
    end

    context "when host app has all pgbus locales available" do
      before do
        allow(I18n).to receive(:available_locales).and_return(pgbus_shipped_locales)
      end

      it "includes all pgbus locales" do
        expect(available_locales).to match_array(pgbus_shipped_locales)
      end
    end

    context "when host app has locales pgbus doesn't ship" do
      before do
        allow(I18n).to receive(:available_locales).and_return(%i[en de zh kr])
      end

      it "only includes the intersection" do
        expect(available_locales).to contain_exactly(:en, :de)
      end
    end

    context "when host app has no available locales" do
      before do
        allow(I18n).to receive(:available_locales).and_return([])
      end

      it "returns an empty array" do
        expect(available_locales).to be_empty
      end
    end
  end
end
