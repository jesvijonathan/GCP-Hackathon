<script>
import { ref, computed } from "vue";

export default {
  name: "ProgressiveForm",
  setup() {
    const currentStep = ref(1);

    const steps = ref([
      { title: "Personal Info" },
      { title: "Address" },
      { title: "Additional" },
    ]);

    const formData = ref({
      firstName: "",
      lastName: "",
      email: "",
      phone: "",
      address: "",
      city: "",
      state: "",
      zipCode: "",
      country: "",
      company: "",
      position: "",
      experience: "",
      interests: [],
      comments: "",
    });

    const isCurrentStepValid = computed(() => {
      switch (currentStep.value) {
        case 1:
          return (
            formData.value.firstName &&
            formData.value.lastName &&
            formData.value.email
          );
        case 2:
          return (
            formData.value.address &&
            formData.value.city &&
            formData.value.state &&
            formData.value.zipCode &&
            formData.value.country
          );
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
        console.log("Form submitted:", formData.value);

        alert("Form submitted successfully!");
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
        <h2 class="form-title">Application Form</h2>
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
        <!-- Step 1: Personal Information -->
        <div v-if="currentStep === 1" class="form-step">
          <h3 class="step-title">Personal Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="firstName">First Name *</label>
              <input
                id="firstName"
                v-model="formData.firstName"
                type="text"
                required
                class="form-input"
                placeholder="Enter your first name"
              />
            </div>
            <div class="form-group">
              <label for="lastName">Last Name *</label>
              <input
                id="lastName"
                v-model="formData.lastName"
                type="text"
                required
                class="form-input"
                placeholder="Enter your last name"
              />
            </div>
            <div class="form-group">
              <label for="email">Email Address *</label>
              <input
                id="email"
                v-model="formData.email"
                type="email"
                required
                class="form-input"
                placeholder="Enter your email address"
              />
            </div>
            <div class="form-group">
              <label for="phone">Phone Number</label>
              <input
                id="phone"
                v-model="formData.phone"
                type="tel"
                class="form-input"
                placeholder="Enter your phone number"
              />
            </div>
          </div>
        </div>

        <!-- Step 2: Address Information -->
        <div v-if="currentStep === 2" class="form-step">
          <h3 class="step-title">Address Information</h3>
          <div class="form-grid">
            <div class="form-group full-width">
              <label for="address">Street Address *</label>
              <input
                id="address"
                v-model="formData.address"
                type="text"
                required
                class="form-input"
                placeholder="Enter your street address"
              />
            </div>
            <div class="form-group">
              <label for="city">City *</label>
              <input
                id="city"
                v-model="formData.city"
                type="text"
                required
                class="form-input"
                placeholder="Enter your city"
              />
            </div>
            <div class="form-group">
              <label for="state">State/Province *</label>
              <input
                id="state"
                v-model="formData.state"
                type="text"
                required
                class="form-input"
                placeholder="Enter your state"
              />
            </div>
            <div class="form-group">
              <label for="zipCode">ZIP Code *</label>
              <input
                id="zipCode"
                v-model="formData.zipCode"
                type="text"
                required
                class="form-input"
                placeholder="Enter your ZIP code"
              />
            </div>
            <div class="form-group">
              <label for="country">Country *</label>
              <select
                id="country"
                v-model="formData.country"
                required
                class="form-input"
              >
                <option value="">Select a country</option>
                <option value="US">United States</option>
                <option value="CA">Canada</option>
                <option value="UK">United Kingdom</option>
                <option value="AU">Australia</option>
                <option value="DE">Germany</option>
                <option value="FR">France</option>
                <option value="other">Other</option>
              </select>
            </div>
          </div>
        </div>

        <!-- Step 3: Additional Information -->
        <div v-if="currentStep === 3" class="form-step">
          <h3 class="step-title">Additional Information</h3>
          <div class="form-grid">
            <div class="form-group">
              <label for="company">Company/Organization</label>
              <input
                id="company"
                v-model="formData.company"
                type="text"
                class="form-input"
                placeholder="Enter your company name"
              />
            </div>
            <div class="form-group">
              <label for="position">Position/Title</label>
              <input
                id="position"
                v-model="formData.position"
                type="text"
                class="form-input"
                placeholder="Enter your position"
              />
            </div>
            <div class="form-group">
              <label for="experience">Years of Experience</label>
              <select
                id="experience"
                v-model="formData.experience"
                class="form-input"
              >
                <option value="">Select experience</option>
                <option value="0-1">0-1 years</option>
                <option value="2-5">2-5 years</option>
                <option value="6-10">6-10 years</option>
                <option value="10+">10+ years</option>
              </select>
            </div>
            <div class="form-group">
              <label for="interests">Areas of Interest</label>
              <select
                id="interests"
                v-model="formData.interests"
                multiple
                class="form-input"
                size="4"
              >
                <option value="technology">Technology</option>
                <option value="finance">Finance</option>
                <option value="marketing">Marketing</option>
                <option value="design">Design</option>
                <option value="consulting">Consulting</option>
                <option value="education">Education</option>
              </select>
            </div>
            <div class="form-group full-width">
              <label for="comments">Additional Comments</label>
              <textarea
                id="comments"
                v-model="formData.comments"
                class="form-input"
                rows="4"
                placeholder="Any additional information you'd like to share..."
              ></textarea>
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
  max-width: 800px;
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

select.form-input[multiple] {
  background: white;
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
