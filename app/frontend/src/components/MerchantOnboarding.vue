<script>
import { ref, computed } from "vue";

export default {
  name: "ProgressiveForm",
  setup() {
    const currentStep = ref(1);

    const steps = ref([
      { title: "Basic Information" },
      { title: "Location & Contact" },
      { title: "Financial & Operational" },
    ]);

    const formData = ref({
      id: "",
      name: "",
      legal_name: "",
      acceptor_name: "",
      acceptor_category_code: "",
      url: "",
      language_code: "",
      time_zone_id: "",
      country_code: "",
      country_sub_division_code: "",
      home_country_code: "",
      region_id: "",
      city: "",
      postal_code: "",
      street: "",
      currency_code: "",
      tax_id: "",
      trade_register_number: "",
      iban: "",
      domiciliary_bank_number: "",
      cut_off_time: "",
      activation_flag: false,
      activation_time: "",
      activation_start_time: "",
      activation_end_time: "",
      city_category_code: "",
      business_service_phone_number: "",
      customer_service_phone_number: "",
      additional_contact_information: "",
    });

    const isCurrentStepValid = computed(() => {
      switch (currentStep.value) {
        case 1:
          return formData.value.id && formData.value.name;
        case 2:
          return true; // No required fields in step 2
        case 3:
          return true; // No required fields in step 3
        default:
          return false;
      }
    });

    const nextStep = () => {
      if (currentStep.value < 3 && isCurrentStepValid.value) {
        currentStep.value++;
      }
    };

    const previousStep = () => {
      if (currentStep.value > 1) {
        currentStep.value--;
      }
    };

    const handleSubmit = () => {
      if (isCurrentStepValid.value) {
        // Create the final JSON object
        const submissionData = {
          ...formData.value,
          // Convert string dates to proper format if needed
          activation_time: formData.value.activation_time || null,
          activation_start_time: formData.value.activation_start_time || null,
          activation_end_time: formData.value.activation_end_time || null,
          cut_off_time: formData.value.cut_off_time || null,
        };

        console.log("Form submitted with data:", JSON.stringify(submissionData, null, 2));
        alert("Form submitted successfully! Check console for JSON data.");
      }
    };

    return {
      currentStep,
      steps,
      formData,
      isCurrentStepValid,
      nextStep,
      previousStep,
      handleSubmit,
    };
  },
};
</script>

<template>
  <div class="progressive-form-container">
    <div class="form-card">
      <!-- Progress Bar -->
      <div class="progress-header">
        <h2 class="form-title">Merchant Registration Form</h2>
        <div class="progress-bar">
          <div class="progress-steps">
            <div
              v-for="(step, index) in steps"
              :key="index"
              class="step"
              :class="{
                active: currentStep === index + 1,
                completed: currentStep > index + 1,
              }"
            >
              <div class="step-circle">
                <i v-if="currentStep > index + 1" class="check-icon">✓</i>
                <span v-else>{{ index + 1 }}</span>
              </div>
              <span class="step-label">{{ step.title }}</span>
            </div>
          </div>
          <div class="progress-line">
            <div
              class="progress-fill"
              :style="{ width: `${((currentStep - 1) / 2) * 100}%` }"
            ></div>
          </div>
        </div>
      </div>

      <!-- Form Content -->
      <form @submit.prevent="handleSubmit" class="form-content">
        <!-- Step 1: Basic Information -->
        <div v-if="currentStep === 1" class="form-step">
          <h3 class="step-title">Basic Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="id">ID *</label>
              <input
                id="id"
                v-model="formData.id"
                type="text"
                required
                maxlength="20"
                class="form-input"
                placeholder="Enter unique ID (max 20 chars)"
              />
            </div>
            <div class="form-group">
              <label for="name">Name *</label>
              <input
                id="name"
                v-model="formData.name"
                type="text"
                required
                maxlength="255"
                class="form-input"
                placeholder="Enter name"
              />
            </div>
            <div class="form-group">
              <label for="legal_name">Legal Name</label>
              <input
                id="legal_name"
                v-model="formData.legal_name"
                type="text"
                maxlength="255"
                class="form-input"
                placeholder="Enter legal name"
              />
            </div>
            <div class="form-group">
              <label for="acceptor_name">Acceptor Name</label>
              <input
                id="acceptor_name"
                v-model="formData.acceptor_name"
                type="text"
                maxlength="255"
                class="form-input"
                placeholder="Enter acceptor name"
              />
            </div>
            <div class="form-group">
              <label for="acceptor_category_code">Acceptor Category Code</label>
              <input
                id="acceptor_category_code"
                v-model="formData.acceptor_category_code"
                type="text"
                maxlength="10"
                class="form-input"
                placeholder="Enter category code"
              />
            </div>
            <div class="form-group">
              <label for="url">Website URL</label>
              <input
                id="url"
                v-model="formData.url"
                type="url"
                maxlength="255"
                class="form-input"
                placeholder="https://example.com"
              />
            </div>
            <div class="form-group">
              <label for="language_code">Language Code</label>
              <select
                id="language_code"
                v-model="formData.language_code"
                class="form-input"
              >
                <option value="">Select language</option>
                <option value="en">English</option>
                <option value="es">Spanish</option>
                <option value="fr">French</option>
                <option value="de">German</option>
                <option value="it">Italian</option>
                <option value="pt">Portuguese</option>
              </select>
            </div>
            <div class="form-group">
              <label for="time_zone_id">Time Zone</label>
              <select
                id="time_zone_id"
                v-model="formData.time_zone_id"
                class="form-input"
              >
                <option value="">Select timezone</option>
                <option value="UTC">UTC</option>
                <option value="America/New_York">Eastern Time</option>
                <option value="America/Chicago">Central Time</option>
                <option value="America/Denver">Mountain Time</option>
                <option value="America/Los_Angeles">Pacific Time</option>
                <option value="Europe/London">London</option>
                <option value="Europe/Paris">Paris</option>
                <option value="Asia/Tokyo">Tokyo</option>
              </select>
            </div>
          </div>
        </div>

        <!-- Step 2: Location & Contact -->
        <div v-if="currentStep === 2" class="form-step">
          <h3 class="step-title">Location & Contact Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="country_code">Country Code</label>
              <input
                id="country_code"
                v-model="formData.country_code"
                type="text"
                maxlength="5"
                class="form-input"
                placeholder="e.g., US, CA, GB"
              />
            </div>
            <div class="form-group">
              <label for="country_sub_division_code">State/Province Code</label>
              <input
                id="country_sub_division_code"
                v-model="formData.country_sub_division_code"
                type="text"
                maxlength="5"
                class="form-input"
                placeholder="e.g., CA, NY, ON"
              />
            </div>
            <div class="form-group">
              <label for="home_country_code">Home Country Code</label>
              <input
                id="home_country_code"
                v-model="formData.home_country_code"
                type="text"
                maxlength="5"
                class="form-input"
                placeholder="Home country code"
              />
            </div>
            <div class="form-group">
              <label for="region_id">Region ID</label>
              <input
                id="region_id"
                v-model="formData.region_id"
                type="text"
                maxlength="20"
                class="form-input"
                placeholder="Enter region ID"
              />
            </div>
            <div class="form-group">
              <label for="city">City</label>
              <input
                id="city"
                v-model="formData.city"
                type="text"
                maxlength="100"
                class="form-input"
                placeholder="Enter city"
              />
            </div>
            <div class="form-group">
              <label for="postal_code">Postal Code</label>
              <input
                id="postal_code"
                v-model="formData.postal_code"
                type="text"
                maxlength="20"
                class="form-input"
                placeholder="Enter postal code"
              />
            </div>
            <div class="form-group full-width">
              <label for="street">Street Address</label>
              <input
                id="street"
                v-model="formData.street"
                type="text"
                maxlength="255"
                class="form-input"
                placeholder="Enter street address"
              />
            </div>
            <div class="form-group">
              <label for="city_category_code">City Category Code</label>
              <input
                id="city_category_code"
                v-model="formData.city_category_code"
                type="text"
                maxlength="10"
                class="form-input"
                placeholder="City category"
              />
            </div>
            <div class="form-group">
              <label for="business_service_phone_number">Business Phone</label>
              <input
                id="business_service_phone_number"
                v-model="formData.business_service_phone_number"
                type="tel"
                maxlength="20"
                class="form-input"
                placeholder="Business phone number"
              />
            </div>
            <div class="form-group">
              <label for="customer_service_phone_number">Customer Service Phone</label>
              <input
                id="customer_service_phone_number"
                v-model="formData.customer_service_phone_number"
                type="tel"
                maxlength="20"
                class="form-input"
                placeholder="Customer service phone"
              />
            </div>
            <div class="form-group full-width">
              <label for="additional_contact_information">Additional Contact Information</label>
              <textarea
                id="additional_contact_information"
                v-model="formData.additional_contact_information"
                class="form-input"
                rows="4"
                placeholder="Any additional contact information..."
              ></textarea>
            </div>
          </div>
        </div>

        <!-- Step 3: Financial & Operational -->
        <div v-if="currentStep === 3" class="form-step">
          <h3 class="step-title">Financial & Operational Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="currency_code">Currency Code</label>
              <select
                id="currency_code"
                v-model="formData.currency_code"
                class="form-input"
              >
                <option value="">Select currency</option>
                <option value="USD">USD - US Dollar</option>
                <option value="EUR">EUR - Euro</option>
                <option value="GBP">GBP - British Pound</option>
                <option value="CAD">CAD - Canadian Dollar</option>
                <option value="JPY">JPY - Japanese Yen</option>
                <option value="AUD">AUD - Australian Dollar</option>
              </select>
            </div>
            <div class="form-group">
              <label for="tax_id">Tax ID</label>
              <input
                id="tax_id"
                v-model="formData.tax_id"
                type="text"
                maxlength="50"
                class="form-input"
                placeholder="Enter tax ID"
              />
            </div>
            <div class="form-group">
              <label for="trade_register_number">Trade Register Number</label>
              <input
                id="trade_register_number"
                v-model="formData.trade_register_number"
                type="text"
                maxlength="50"
                class="form-input"
                placeholder="Trade register number"
              />
            </div>
            <div class="form-group">
              <label for="iban">IBAN</label>
              <input
                id="iban"
                v-model="formData.iban"
                type="text"
                maxlength="34"
                class="form-input"
                placeholder="International Bank Account Number"
              />
            </div>
            <div class="form-group">
              <label for="domiciliary_bank_number">Domiciliary Bank Number</label>
              <input
                id="domiciliary_bank_number"
                v-model="formData.domiciliary_bank_number"
                type="text"
                maxlength="50"
                class="form-input"
                placeholder="Bank number"
              />
            </div>
            <div class="form-group">
              <label for="cut_off_time">Cut-off Time</label>
              <input
                id="cut_off_time"
                v-model="formData.cut_off_time"
                type="time"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label for="activation_flag">Activation Status</label>
              <div class="checkbox-wrapper">
                <input
                  id="activation_flag"
                  v-model="formData.activation_flag"
                  type="checkbox"
                  class="form-checkbox"
                />
                <label for="activation_flag" class="checkbox-label">Active</label>
              </div>
            </div>
            <div class="form-group">
              <label for="activation_time">Activation Time</label>
              <input
                id="activation_time"
                v-model="formData.activation_time"
                type="datetime-local"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label for="activation_start_time">Activation Start Time</label>
              <input
                id="activation_start_time"
                v-model="formData.activation_start_time"
                type="datetime-local"
                class="form-input"
              />
            </div>
            <div class="form-group">
              <label for="activation_end_time">Activation End Time</label>
              <input
                id="activation_end_time"
                v-model="formData.activation_end_time"
                type="datetime-local"
                class="form-input"
              />
            </div>
          </div>
        </div>

        <!-- Navigation Buttons -->
        <div class="form-navigation">
          <button
            v-if="currentStep > 1"
            type="button"
            @click="previousStep"
            class="btn btn-secondary"
          >
            Previous
          </button>
          <div class="spacer"></div>
          <button
            v-if="currentStep < 3"
            type="button"
            @click="nextStep"
            class="btn btn-primary"
            :disabled="!isCurrentStepValid"
          >
            Next
          </button>
          <button
            v-if="currentStep === 3"
            type="submit"
            class="btn btn-primary"
            :disabled="!isCurrentStepValid"
          >
            Submit
          </button>
        </div>
      </form>
    </div>
  </div>
</template>

<style scoped>
.progressive-form-container {
  min-height: 100vh;
  background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
  padding: 2rem;
  display: flex;
  justify-content: center;
  align-items: flex-start;
  font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
}

.form-card {
  background: white;
  border-radius: 16px;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
  max-width: 900px;
  width: 100%;
  overflow: hidden;
}

.progress-header {
  background: linear-gradient(135deg, #008080 0%, #20b2aa 100%);
  color: white;
  padding: 2rem;
  text-align: center;
}

.form-title {
  margin: 0 0 2rem 0;
  font-size: 2rem;
  font-weight: 300;
}

.progress-bar {
  position: relative;
}

.progress-steps {
  display: flex;
  justify-content: space-between;
  position: relative;
  z-index: 2;
}

.step {
  display: flex;
  flex-direction: column;
  align-items: center;
  flex: 1;
}

.step-circle {
  width: 40px;
  height: 40px;
  border-radius: 50%;
  background: rgba(255, 255, 255, 0.3);
  display: flex;
  align-items: center;
  justify-content: center;
  font-weight: bold;
  margin-bottom: 0.5rem;
  transition: all 0.3s ease;
  border: 2px solid rgba(255, 255, 255, 0.5);
}

.step.active .step-circle {
  background: white;
  color: #008080;
  border-color: white;
}

.step.completed .step-circle {
  background: #20b2aa;
  color: white;
  border-color: #20b2aa;
}

.check-icon {
  font-size: 1.2rem;
}

.step-label {
  font-size: 0.9rem;
  opacity: 0.8;
}

.step.active .step-label {
  opacity: 1;
  font-weight: 500;
}

.progress-line {
  position: absolute;
  top: 20px;
  left: 0;
  right: 0;
  height: 2px;
  background: rgba(255, 255, 255, 0.3);
  z-index: 1;
}

.progress-fill {
  height: 100%;
  background: white;
  transition: width 0.3s ease;
}

.form-content {
  padding: 2rem;
}

.form-step {
  min-height: 400px;
}

.step-title {
  color: #333;
  margin-bottom: 2rem;
  font-size: 1.5rem;
  font-weight: 400;
  text-align: center;
}

.form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1.5rem;
}

.form-group {
  display: flex;
  flex-direction: column;
}

.form-group.full-width {
  grid-column: 1 / -1;
}

.form-group label {
  margin-bottom: 0.5rem;
  color: #555;
  font-weight: 500;
  font-size: 0.9rem;
}

.form-input {
  padding: 0.75rem 1rem;
  border: 2px solid #e1e5e9;
  border-radius: 8px;
  font-size: 1rem;
  transition: all 0.3s ease;
  background: #fafbfc;
}

.form-input:focus {
  outline: none;
  border-color: #008080;
  background: white;
  box-shadow: 0 0 0 3px rgba(0, 128, 128, 0.1);
}

.form-input:invalid {
  border-color: #e74c3c;
}

textarea.form-input {
  resize: vertical;
  min-height: 100px;
}

.checkbox-wrapper {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
}

.form-checkbox {
  width: 18px;
  height: 18px;
  margin: 0;
}

.checkbox-label {
  margin: 0;
  cursor: pointer;
}

.form-navigation {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 3rem;
  padding-top: 2rem;
  border-top: 1px solid #e1e5e9;
}

.spacer {
  flex: 1;
}

.btn {
  padding: 0.75rem 2rem;
  border: none;
  border-radius: 8px;
  font-size: 1rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.3s ease;
  min-width: 120px;
}

.btn-primary {
  background: #008080;
  color: white;
}

.btn-primary:hover:not(:disabled) {
  background: #006666;
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 128, 128, 0.3);
}

.btn-secondary {
  background: #6c757d;
  color: white;
}

.btn-secondary:hover {
  background: #5a6268;
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(108, 117, 125, 0.3);
}

.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
  transform: none !important;
  box-shadow: none !important;
}

@media (max-width: 768px) {
  .progressive-form-container {
    padding: 1rem;
  }

  .form-grid {
    grid-template-columns: 1fr;
  }

  .progress-steps {
    flex-direction: column;
    gap: 1rem;
  }

  .step {
    flex-direction: row;
    justify-content: flex-start;
    gap: 1rem;
  }

  .step-circle {
    margin-bottom: 0;
    width: 35px;
    height: 35px;
  }

  .progress-line {
    display: none;
  }
}
</style>