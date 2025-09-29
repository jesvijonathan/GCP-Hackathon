<template>
  <div class="page-header">
    <nav class="breadcrumb">
      <router-link to="/dashboard" class="breadcrumb-item">Dashboard</router-link>
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item">Merchants</span>
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item current">
        {{ merchant?.name || routeIdentifier || "Loading..." }}
      </span>
    </nav>
  </div>

  <div class="merchant-detail-container">
    <div v-if="loading" class="loading">Loading merchant details...</div>

    <div v-if="merchant && !loading" class="layout">
      <!-- Side summary panel -->
      <aside class="side-panel">
        <div class="merchant-header">
          <div class="title-row">
            <span class="status-dot" :class="{ active: isActive, inactive: !isActive }"></span>
            <h1 class="merchant-title">{{ merchant.name || "Merchant" }}</h1>
          </div>
          <p class="merchant-id">ID: {{ merchant.id || "-" }}</p>
        </div>

        <div class="summary-list">
          <div class="summary-item">
            <span class="label">Region</span>
            <span class="value">{{ merchant.region_id || "-" }}</span>
          </div>
          <div class="summary-item">
            <span class="label">Timezone</span>
            <span class="value">{{ merchant.time_zone_id || "-" }}</span>
          </div>
          <div class="summary-item">
            <span class="label">Status</span>
            <span class="value">{{ isActive ? "Active" : "Inactive" }}</span>
          </div>
        </div>

        <!-- Navigation / Notifications -->
        <div class="panel-section">
          <div class="panel-actions">
            <div @click="openEmailBox" class="notification">Notifications</div>
            <div @click="openExplore" class="notification">Explore</div>
          </div>
        </div>

        <!-- Auto refresh -->
        <div class="panel-section">
          <div class="action-center-dropdown">
            <label class="checkbox-label">
              <input v-model="autoRefresh" type="checkbox" class="form-checkbox" />
              Auto refresh
            </label>
            <div v-if="autoRefresh" class="form-group">
              <label for="refreshSec">Refresh interval (seconds)</label>
              <input id="refreshSec" v-model.number="refreshSec" type="number" min="2" class="form-input" />
            </div>
          </div>
        </div>

        <!-- Admin actions -->
        <div class="panel-section action-center-dropdown">
          <button @click="openPermanentBanModal">Permanent Ban</button>
          <button @click="openShadowBanModal">Shadow Ban</button>
          <button @click="openRestrictModal">Restrict Merchant</button>
        </div>

        <!-- Save -->
        <div class="panel-section action-center-dropdown">
          <button class="btn btn-primary primary-btn" @click="saveUpdates" :disabled="!hasChanges || saving">
            {{ saving ? "Saving..." : "Update Changes" }}
          </button>
        </div>

        <div class="actions-taken">
          <h1>Actions Taken On {{ merchant.name }}</h1>
          <p class="actions-taken-data">
            {{ actionsTaken.length > 0 ? actionsTaken.join(", ") : "No actions taken" }}
          </p>
        </div>
      </aside>

      <!-- Main cards -->

      <main class="merchant-detail">
        <section class="card">
          <div class="card-header">
            <h3>Twitter Data Pipeline</h3>
          </div>

          <!-- pass the json to the merchant_twitter component -->
          <div class="card-body">
            <MerchantTwitter :tweets="tweets" :loading="tweetsLoading" :unit="unit" />
          </div>
        </section>




        <section class="card">
          <div class="card-header">
        <h3>Merchant Actions & Restrictions</h3>
          </div>
          <div class="card-body" style="display:flex;flex-direction:column;gap:18px;">

        <!-- Current Action Chips -->
        <div>
          <div style="font-weight:600;font-size:13px;color:#6b7280;margin-bottom:6px;">Current Actions</div>
          <div v-if="actionsTaken.length" style="display:flex;flex-wrap:wrap;gap:6px;">
            <span
          v-for="(a,i) in actionsTaken"
          :key="i"
          :title="a"
          style="background:#f0fdfa;color:#008080;border:1px solid #14b8a6;padding:4px 10px;border-radius:20px;font-size:12px;font-weight:600;line-height:1;"
            >{{ a }}</span>
          </div>
          <div v-else style="font-size:13px;color:#9ca3af;font-style:italic;">No actions or restrictions in place.</div>
        </div>

        <!-- Status Summary -->
        <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;">
          <div style="background:#f9fafb;border:1px solid #e5e7eb;padding:10px;border-radius:6px;">
            <div style="font-size:11px;font-weight:700;color:#6b7280;letter-spacing:.5px;">ENFORCEMENT STATUS</div>
            <div style="margin-top:4px;font-weight:600;color:#111827;">
          <template v-if="actionsTaken.includes('Permanent Ban')">Permanently Banned</template>
          <template v-else-if="actionsTaken.includes('Shadow Ban')">Shadow Banned</template>
          <template v-else-if="actionsTaken.includes('Restricted')">Restricted</template>
          <template v-else>{{ isActive ? 'Active' : 'Inactive' }}</template>
            </div>
          </div>

          <div v-if="actionsTaken.includes('Permanent Ban')" style="background:#fff1f2;border:1px solid #fecdd3;padding:10px;border-radius:6px;">
            <div style="font-size:11px;font-weight:700;color:#b91c1c;">PERMANENT BAN</div>
            <div style="margin-top:4px;font-size:13px;color:#7f1d1d;">All activity disabled.</div>
          </div>

          <div v-else-if="actionsTaken.includes('Shadow Ban')" style="background:#fff7ed;border:1px solid #fed7aa;padding:10px;border-radius:6px;">
            <div style="font-size:11px;font-weight:700;color:#c2410c;">SHADOW BAN</div>
            <div style="margin-top:4px;font-size:13px;color:#7c2d12;">Visibility & capabilities reduced.</div>
          </div>
        </div>

        <!-- Restriction Detail Block -->
        <div
          v-if="actionsTaken.includes('Restricted')"
          style="display:flex;flex-direction:column;gap:14px;background:#f9fafb;border:1px solid #e5e7eb;padding:14px 16px;border-radius:8px;"
        >
          <div style="display:flex;align-items:center;gap:8px;">
            <span style="font-size:12px;font-weight:700;color:#6b7280;letter-spacing:.5px;">RESTRICTION DETAILS</span>
          </div>

          <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;">
            <div v-if="restrictionData.dailyTransactionLimit != null">
          <div style="font-size:11px;color:#6b7280;font-weight:600;">Daily Limit</div>
          <div style="font-weight:600;">${{ restrictionData.dailyTransactionLimit }}</div>
            </div>
            <div v-if="restrictionData.monthlyTransactionLimit != null">
          <div style="font-size:11px;color:#6b7280;font-weight:600;">Monthly Limit</div>
          <div style="font-weight:600;">${{ restrictionData.monthlyTransactionLimit }}</div>
            </div>
            <div v-if="restrictionData.maxTransactionAmount != null">
          <div style="font-size:11px;color:#6b7280;font-weight:600;">Max Single Tx</div>
          <div style="font-weight:600;">${{ restrictionData.maxTransactionAmount }}</div>
            </div>
            <div v-if="restrictionData.allowedCountries">
          <div style="font-size:11px;color:#6b7280;font-weight:600;">Allowed Countries</div>
          <div style="font-weight:600;">{{ restrictionData.allowedCountries }}</div>
            </div>
            <div>
          <div style="font-size:11px;color:#6b7280;font-weight:600;">Intl Transactions</div>
          <div style="font-weight:600;">
            {{ restrictionData.blockInternationalTransactions ? 'Blocked' : 'Allowed' }}
          </div>
            </div>
            <div>
          <div style="font-size:11px;color:#6b7280;font-weight:600;">Extra Verification</div>
          <div style="font-weight:600;">
            {{ restrictionData.requireAdditionalVerification ? 'Required' : 'Not Required' }}
          </div>
            </div>
            <div v-if="restrictionData.startDate">
          <div style="font-size:11px;color:#6b7280;font-weight:600;">Start Date</div>
          <div style="font-weight:600;">{{ restrictionData.startDate }}</div>
            </div>
            <div v-if="restrictionData.endDate">
          <div style="font-size:11px;color:#6b7280;font-weight:600;">End Date</div>
          <div style="font-weight:600;">{{ restrictionData.endDate }}</div>
            </div>
          </div>

          <div v-if="restrictionData.restrictedCategoryCodes.length">
            <div style="font-size:11px;color:#6b7280;font-weight:600;margin-bottom:4px;">Restricted MCCs</div>
            <div style="display:flex;flex-wrap:wrap;gap:6px;">
          <span
            v-for="c in restrictionData.restrictedCategoryCodes"
            :key="c"
            style="background:#eef2ff;color:#3730a3;border:1px solid #c7d2fe;padding:4px 8px;font-size:11px;font-weight:600;border-radius:4px;"
          >{{ c }}</span>
            </div>
          </div>

          <div v-if="restrictionData.reason">
            <div style="font-size:11px;color:#6b7280;font-weight:600;margin-bottom:4px;">Reason</div>
            <div style="font-size:13px;white-space:pre-line;">{{ restrictionData.reason }}</div>
          </div>
        </div>

          </div>
        </section>
      
  <!-- Single, comprehensive card for all merchant info -->
  <section class="card">
    <div class="card-header">
      <h3>Merchant Details</h3>
    </div>

    <div class="card-body grid-3">

      <!-- display all merchant data here
       
      {
  "_id": {
    "$oid": "68da23f1ebc2a078aa28d477"
  },
  "merchant_name": "HomeGear",
  "acceptor_category_code": "4900",
  "acceptor_name": "HomeGear Acceptor",
  "activation_end_time": null,
  "activation_flag": false,
  "activation_start_time": "2024-06-02T06:15:13.213876Z",
  "activation_time": "2025-03-14T06:15:13.213876Z",
  "additional_contact_information": "Contact support via portal.",
  "business_service_phone_number": "+68-399-456-3729",
  "city": "Paris",
  "city_category_code": null,
  "country_code": "IT",
  "country_sub_division_code": null,
  "created_at": "2025-09-29T06:15:13.213931Z",
  "currency_code": "EUR",
  "customer_service_phone_number": "+40-414-437-3858",
  "cut_off_time": "01:18:30",
  "description": "Acquirer/acceptor profile for merchant risk simulation.",
  "domiciliary_bank_number": "BANK-766653",
  "end_date": "2026-09-29",
  "home_country_code": "IT",
  "iban": null,
  "language_code": "en",
  "legal_name": "HomeGear Ltd.",
  "merchant_code": "42597900",
  "merchant_id": "acc_9dddd65872",
  "postal_code": "25141",
  "region_id": "EU",
  "start_date": "2022-09-29",
  "street": "193 Broadway",
  "tax_id": "TAX-CF1F1CE7",
  "time_zone_id": "Europe/Rome",
  "trade_register_number": "REG-EE2A0FB7",
  "updated_at": "2025-09-29T06:15:13.213943Z",
  "url": "https://homegear.example.com"
}
      
      -->
      <div
        class="card-footer meta-footer meta-grid"
        style="
          grid-column: 1 / -1;
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
          gap: 20px 24px;
          padding-top: 14px;
        "
      >

        <!-- Read-only -->
        <div class="meta-item read-only" style="display:flex;flex-direction:column;gap:4px;">
          <div class="meta-label">Merchant ID</div>
          <div class="meta-value">{{ merchant.id || 'N/A' }}</div>
        </div>

        <!-- Merchant Code -->
        <div class="meta-item" @click="!editFields.merchant_code && (editFields.merchant_code=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Merchant Code</div>
          <div v-if="!editFields.merchant_code" class="meta-value">{{ merchant.merchant_code || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.merchant_code"
             @keydown.enter.prevent="editFields.merchant_code=false"
             @blur="editFields.merchant_code=false" />
        </div>

        <!-- URL -->
        <div class="meta-item" @click="!editFields.url && (editFields.url=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">URL</div>
          <div v-if="!editFields.url" class="meta-value">
        <template v-if="merchant.url">
          <a :href="merchant.url" target="_blank" rel="noopener">{{ merchant.url }}</a>
        </template>
        <template v-else>N/A</template>
          </div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.url"
             @keydown.enter.prevent="editFields.url=false"
             @blur="editFields.url=false" />
        </div>

        <!-- Legal Name -->
        <div class="meta-item" @click="!editFields.legal_name && (editFields.legal_name=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Legal Name</div>
        <div v-if="!editFields.legal_name" class="meta-value">{{ merchant.legal_name || 'N/A' }}</div>
        <input v-else autofocus class="form-input" type="text" v-model="merchant.legal_name"
           @keydown.enter.prevent="editFields.legal_name=false"
           @blur="editFields.legal_name=false" />
        </div>

        <!-- Acceptor Name -->
        <div class="meta-item" @click="!editFields.acceptor_name && (editFields.acceptor_name=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Acceptor Name</div>
          <div v-if="!editFields.acceptor_name" class="meta-value">{{ merchant.acceptor_name || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.acceptor_name"
             @keydown.enter.prevent="editFields.acceptor_name=false"
             @blur="editFields.acceptor_name=false" />
        </div>

        <!-- Category Code -->
        <div class="meta-item" @click="!editFields.acceptor_category_code && (editFields.acceptor_category_code=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Category Code</div>
          <div v-if="!editFields.acceptor_category_code" class="meta-value">
        {{ merchant.acceptor_category_code || 'N/A' }} <span v-if="mccLabel">{{ mccLabel }}</span>
          </div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.acceptor_category_code"
             @keydown.enter.prevent="editFields.acceptor_category_code=false"
             @blur="editFields.acceptor_category_code=false" />
        </div>

        <!-- Country -->
        <div class="meta-item" @click="!editFields.country_code && (editFields.country_code=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Country</div>
          <div v-if="!editFields.country_code" class="meta-value">{{ merchant.country_code || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.country_code"
             @keydown.enter.prevent="editFields.country_code=false"
             @blur="editFields.country_code=false" />
        </div>

        <!-- Region -->
        <div class="meta-item" @click="!editFields.region_id && (editFields.region_id=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Region</div>
          <div v-if="!editFields.region_id" class="meta-value">{{ merchant.region_id || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.region_id"
             @keydown.enter.prevent="editFields.region_id=false"
             @blur="editFields.region_id=false" />
        </div>

        <!-- Street -->
        <div class="meta-item" @click="!editFields.street && (editFields.street=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Street</div>
          <div v-if="!editFields.street" class="meta-value">{{ merchant.street || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.street"
             @keydown.enter.prevent="editFields.street=false"
             @blur="editFields.street=false" />
        </div>

        <!-- City -->
        <div class="meta-item" @click="!editFields.city && (editFields.city=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">City</div>
          <div v-if="!editFields.city" class="meta-value">{{ merchant.city || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.city"
             @keydown.enter.prevent="editFields.city=false"
             @blur="editFields.city=false" />
        </div>

        <!-- Postal Code -->
        <div class="meta-item" @click="!editFields.postal_code && (editFields.postal_code=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Postal Code</div>
          <div v-if="!editFields.postal_code" class="meta-value">{{ merchant.postal_code || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.postal_code"
             @keydown.enter.prevent="editFields.postal_code=false"
             @blur="editFields.postal_code=false" />
        </div>

        <!-- Language -->
        <div class="meta-item" @click="!editFields.language_code && (editFields.language_code=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Language</div>
          <div v-if="!editFields.language_code" class="meta-value">{{ merchant.language_code || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.language_code"
             @keydown.enter.prevent="editFields.language_code=false"
             @blur="editFields.language_code=false" />
        </div>

        <!-- Timezone -->
        <div class="meta-item" @click="!editFields.time_zone_id && (editFields.time_zone_id=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Timezone</div>
          <div v-if="!editFields.time_zone_id" class="meta-value">{{ merchant.time_zone_id || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.time_zone_id"
             @keydown.enter.prevent="editFields.time_zone_id=false"
             @blur="editFields.time_zone_id=false" />
        </div>

        <!-- Business Phone -->
        <div class="meta-item" @click="!editFields.business_service_phone_number && (editFields.business_service_phone_number=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Business Phone</div>
          <div v-if="!editFields.business_service_phone_number" class="meta-value">{{ merchant.business_service_phone_number || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.business_service_phone_number"
             @keydown.enter.prevent="editFields.business_service_phone_number=false"
             @blur="editFields.business_service_phone_number=false" />
        </div>

        <!-- Customer Phone -->
        <div class="meta-item" @click="!editFields.customer_service_phone_number && (editFields.customer_service_phone_number=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Customer Phone</div>
          <div v-if="!editFields.customer_service_phone_number" class="meta-value">{{ merchant.customer_service_phone_number || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.customer_service_phone_number"
             @keydown.enter.prevent="editFields.customer_service_phone_number=false"
             @blur="editFields.customer_service_phone_number=false" />
        </div>

        <!-- Additional Contact -->
        <div class="meta-item" @click="!editFields.additional_contact_information && (editFields.additional_contact_information=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Additional Contact Info</div>
          <div v-if="!editFields.additional_contact_information" class="meta-value" style="white-space:pre-line;">
        {{ merchant.additional_contact_information || 'N/A' }}
          </div>
          <textarea v-else autofocus class="form-textarea" rows="3" v-model="merchant.additional_contact_information"
            @blur="editFields.additional_contact_information=false"></textarea>
        </div>

        <!-- Currency -->
        <div class="meta-item" @click="!editFields.currency_code && (editFields.currency_code=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Currency</div>
          <div v-if="!editFields.currency_code" class="meta-value">{{ merchant.currency_code || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.currency_code"
             @keydown.enter.prevent="editFields.currency_code=false"
             @blur="editFields.currency_code=false" />
        </div>

        <!-- Tax ID -->
        <div class="meta-item" @click="!editFields.tax_id && (editFields.tax_id=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Tax ID</div>
          <div v-if="!editFields.tax_id" class="meta-value">{{ merchant.tax_id || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.tax_id"
             @keydown.enter.prevent="editFields.tax_id=false"
             @blur="editFields.tax_id=false" />
        </div>

        <!-- Trade Register -->
        <div class="meta-item" @click="!editFields.trade_register_number && (editFields.trade_register_number=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Trade Register No</div>
          <div v-if="!editFields.trade_register_number" class="meta-value">{{ merchant.trade_register_number || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.trade_register_number"
             @keydown.enter.prevent="editFields.trade_register_number=false"
             @blur="editFields.trade_register_number=false" />
        </div>

        <!-- IBAN -->
        <div class="meta-item" @click="!editFields.iban && (editFields.iban=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">IBAN</div>
          <div v-if="!editFields.iban" class="meta-value">{{ merchant.iban || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.iban"
             @keydown.enter.prevent="editFields.iban=false"
             @blur="editFields.iban=false" />
        </div>

        <!-- Domiciliary Bank -->
        <div class="meta-item" @click="!editFields.domiciliary_bank_number && (editFields.domiciliary_bank_number=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Domiciliary Bank No</div>
          <div v-if="!editFields.domiciliary_bank_number" class="meta-value">{{ merchant.domiciliary_bank_number || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="text" v-model="merchant.domiciliary_bank_number"
             @keydown.enter.prevent="editFields.domiciliary_bank_number=false"
             @blur="editFields.domiciliary_bank_number=false" />
        </div>

        <!-- Cut-off Time -->
        <div class="meta-item" @click="!editFields.cut_off_time && (editFields.cut_off_time=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Cut-off Time</div>
          <div v-if="!editFields.cut_off_time" class="meta-value">{{ merchant.cut_off_time || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="time" v-model="merchant.cut_off_time"
             @keydown.enter.prevent="editFields.cut_off_time=false"
             @blur="editFields.cut_off_time=false" />
        </div>

        <!-- Activation Flag -->
        <div class="meta-item" @click="!editFields.activation_flag && (editFields.activation_flag=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Activation Flag</div>
          <div v-if="!editFields.activation_flag" class="meta-value">
        {{ merchant.activation_flag ? 'True' : 'False' }}
          </div>
          <select v-else autofocus class="form-input" v-model="merchant.activation_flag"
          @blur="editFields.activation_flag=false">
        <option :value="true">Enabled</option>
        <option :value="false">Disabled</option>
          </select>
        </div>

        <!-- Activation Time -->
        <div class="meta-item" @click="!editFields.activation_time && (editFields.activation_time=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Activation Time</div>
          <div v-if="!editFields.activation_time" class="meta-value">{{ formatDate(merchant.activation_time) }}</div>
          <input v-else autofocus class="form-input" type="datetime-local" v-model="merchant.activation_time"
             @keydown.enter.prevent="editFields.activation_time=false"
             @blur="editFields.activation_time=false" />
        </div>

        <!-- Activation Start -->
        <div class="meta-item" @click="!editFields.activation_start_time && (editFields.activation_start_time=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Activation Start</div>
          <div v-if="!editFields.activation_start_time" class="meta-value">{{ formatDate(merchant.activation_start_time) }}</div>
          <input v-else autofocus class="form-input" type="datetime-local" v-model="merchant.activation_start_time"
             @keydown.enter.prevent="editFields.activation_start_time=false"
             @blur="editFields.activation_start_time=false" />
        </div>

        <!-- Activation End -->
        <div class="meta-item" @click="!editFields.activation_end_time && (editFields.activation_end_time=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Activation End</div>
          <div v-if="!editFields.activation_end_time" class="meta-value">{{ formatDate(merchant.activation_end_time) }}</div>
          <input v-else autofocus class="form-input" type="datetime-local" v-model="merchant.activation_end_time"
             @keydown.enter.prevent="editFields.activation_end_time=false"
             @blur="editFields.activation_end_time=false" />
        </div>

        <!-- Start Date -->
        <div class="meta-item" @click="!editFields.start_date && (editFields.start_date=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">Start Date</div>
          <div v-if="!editFields.start_date" class="meta-value">{{ merchant.start_date || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="date" v-model="merchant.start_date"
             @keydown.enter.prevent="editFields.start_date=false"
             @blur="editFields.start_date=false" />
        </div>

        <!-- End Date -->
        <div class="meta-item" @click="!editFields.end_date && (editFields.end_date=true)" style="display:flex;flex-direction:column;gap:4px;cursor:pointer;">
          <div class="meta-label">End Date</div>
          <div v-if="!editFields.end_date" class="meta-value">{{ merchant.end_date || 'N/A' }}</div>
          <input v-else autofocus class="form-input" type="date" v-model="merchant.end_date"
             @keydown.enter.prevent="editFields.end_date=false"
             @blur="editFields.end_date=false" />
        </div>

        <!-- Description -->
        <div class="meta-item" @click="!editFields.description && (editFields.description=true)" style="display:flex;flex-direction:column;gap:6px;cursor:pointer;">
          <div class="meta-label">Description</div>
          <div v-if="!editFields.description" class="meta-value" style="white-space:pre-line;">
        {{ merchant.description || 'N/A' }}
          </div>
          <textarea v-else autofocus class="form-textarea" rows="4" v-model="merchant.description"
            @blur="editFields.description=false"></textarea>
        </div>

        <!-- Created (read-only) -->
        <div class="meta-item read-only" style="display:flex;flex-direction:column;gap:4px;">
          <div class="meta-label">Created</div>
          <div class="meta-value">{{ formatDate(merchant.created_at) }}</div>
        </div>

        <!-- Updated (read-only) -->
        <div class="meta-item read-only" style="display:flex;flex-direction:column;gap:4px;">
          <div class="meta-label">Updated</div>
          <div class="meta-value">{{ formatDate(merchant.updated_at) }}</div>
        </div>

      </div>
      </div>
  </section>
</main>
    </div>

    <div v-else-if="!loading" class="error">
      <h2>Merchant not found</h2>
      <p>The merchant "{{ routeIdentifier }}" could not be found.</p>
      <button @click="goBack" class="btn btn-secondary">← Back to Dashboard</button>
    </div>

    <!-- Permanent Ban Modal -->
    <div v-if="showPermanentBanModal" class="modal-overlay" @click="closePermanentBanModal">
      <div class="modal-content" @click.stop>
        <div class="modal-header">
          <h3>Confirm Permanent Ban</h3>
          <button @click="closePermanentBanModal" class="close-btn">&times;</button>
        </div>
        <div class="modal-body">
          <div class="warning-icon">⚠️</div>
          <p class="warning-text">Are you sure you want to permanently ban <strong>{{ merchant?.name }}</strong>?</p>
          <p class="warning-subtext">This action cannot be undone.</p>
        </div>
        <div class="modal-footer">
          <button @click="closePermanentBanModal" class="btn-cancel">Cancel</button>
          <button @click="confirmPermanentBan" class="btn-danger">Permanent Ban</button>
        </div>
      </div>
    </div>

    <!-- Shadow Ban Modal -->
    <div v-if="showShadowBanModal" class="modal-overlay" @click="closeShadowBanModal">
      <div class="modal-content" @click.stop>
        <div class="modal-header">
          <h3>Confirm Shadow Ban</h3>
          <button @click="closeShadowBanModal" class="close-btn">&times;</button>
        </div>
        <div class="modal-body">
          <div class="warning-icon">🔒</div>
          <p class="warning-text">Are you sure you want to shadow ban <strong>{{ merchant?.name }}</strong>?</p>
          <p class="warning-subtext">Limited visibility and functionality.</p>
        </div>
        <div class="modal-footer">
          <button @click="closeShadowBanModal" class="btn-cancel">Cancel</button>
          <button @click="confirmShadowBan" class="btn-warning">Shadow Ban</button>
        </div>
      </div>
    </div>

    <!-- Restrict Modal -->
    <div v-if="showRestrictModal" class="modal-overlay" @click="closeRestrictModal">
      <div class="modal-content large" @click.stop>
        <div class="modal-header">
          <h3>Restrict Merchant: {{ merchant?.name }}</h3>
          <button @click="closeRestrictModal" class="close-btn">&times;</button>
        </div>
        <div class="modal-body">
          <form @submit.prevent="confirmRestriction" class="restriction-form">
            <div class="form-section">
              <h4>Transaction Limits</h4>
              <div class="form-row">
                <div class="form-group">
                  <label for="dailyLimit">Daily Transaction Limit ($)</label>
                  <input id="dailyLimit" v-model.number="restrictionData.dailyTransactionLimit" type="number" min="0" class="form-input" />
                </div>
                <div class="form-group">
                  <label for="monthlyLimit">Monthly Transaction Limit ($)</label>
                  <input id="monthlyLimit" v-model.number="restrictionData.monthlyTransactionLimit" type="number" min="0" class="form-input" />
                </div>
              </div>
            </div>

            <div class="form-section">
              <h4>Restricted Categories (MCC)</h4>
              <div class="form-group">
                <div class="category-selector">
                  <div class="selected-categories">
                    <span v-for="code in restrictionData.restrictedCategoryCodes" :key="code" class="category-tag">
                      {{ code }}
                      <button type="button" @click="removeCategoryCode(code)" class="remove-tag">&times;</button>
                    </span>
                  </div>
                  <select v-model="selectedCategoryCode" @change="addCategoryCode" class="form-select">
                    <option value="">Select category</option>
                    <option value="5541">5541 Gas Stations</option>
                    <option value="5542">5542 Automated Fuel</option>
                    <option value="5812">5812 Restaurants</option>
                    <option value="5814">5814 Fast Food</option>
                    <option value="5921">5921 Liquor Stores</option>
                    <option value="5993">5993 Cigar Stores</option>
                    <option value="6010">6010 Financial Institutions</option>
                    <option value="6011">6011 ATMs</option>
                    <option value="7995">7995 Gambling</option>
                  </select>
                </div>
              </div>
            </div>

            <div class="form-section">
              <h4>Additional Restrictions</h4>
              <div class="form-group">
                <label for="maxTransactionAmount">Max Single Transaction ($)</label>
                <input id="maxTransactionAmount" v-model.number="restrictionData.maxTransactionAmount" type="number" min="0" class="form-input" />
              </div>
              <div class="form-group">
                <label for="allowedCountries">Allowed Countries</label>
                <input id="allowedCountries" v-model="restrictionData.allowedCountries" type="text" class="form-input" placeholder="e.g., US, CA, GB" />
              </div>
              <div class="form-group">
                <label class="checkbox-label">
                  <input v-model="restrictionData.requireAdditionalVerification" type="checkbox" class="form-checkbox" />
                  Require additional verification
                </label>
              </div>
              <div class="form-group">
                <label class="checkbox-label">
                  <input v-model="restrictionData.blockInternationalTransactions" type="checkbox" class="form-checkbox" />
                  Block international transactions
                </label>
              </div>
            </div>

            <div class="form-section">
              <h4>Reason</h4>
              <div class="form-group">
                <label for="reason">Reason</label>
                <textarea id="reason" v-model="restrictionData.reason" rows="3" class="form-textarea" placeholder="Explain the reason..." required></textarea>
              </div>
            </div>

            <div class="form-section">
              <h4>Duration</h4>
              <div class="form-row">
                <div class="form-group">
                  <label for="startDate">Start Date</label>
                  <input id="startDate" v-model="restrictionData.startDate" type="date" class="form-input" required />
                </div>
                <div class="form-group">
                  <label for="endDate">End Date (optional)</label>
                  <input id="endDate" v-model="restrictionData.endDate" type="date" class="form-input" />
                </div>
              </div>
            </div>
          </form>
        </div>
        <div class="modal-footer">
          <button @click="closeRestrictModal" class="btn-cancel">Cancel</button>
          <button @click="confirmRestriction" class="btn-primary">Apply Restrictions</button>
        
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { ref, reactive, computed, onMounted, watch, nextTick } from "vue";
import { useRoute, useRouter } from "vue-router";
import { toast } from "vue3-toastify";
import "vue3-toastify/dist/index.css";
import MerchantTwitter from "./MerchantTwitter.vue";

// Centralized API config
const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

// -------------------------------------
// Utilities
// -------------------------------------
function toHHMMSS(val) {
  if (!val) return null;
  const p = String(val).split(":");
  const hh = (p[0] || "00").padStart(2, "0");
  const mm = String(p[1] || "00").padStart(2, "0");
  const ss = String(p[2] || "00").padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}
function isoToLocal(iso) {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    const hh = String(d.getHours()).padStart(2, "0");
    const mi = String(d.getMinutes()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd}T${hh}:${mi}`;
  } catch {
    return "";
  }
}
function formatDate(iso) {
  if (!iso) return "N/A";
  try {
    const d = new Date(iso);
    return d.toLocaleDateString("en-US", {
      year: "numeric",
      month: "long",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}
async function apiGet(path, params = undefined, signal = undefined) {
  const qs =
    params && Object.keys(params).length
      ? `?${new URLSearchParams(params).toString()}`
      : "";
  const resp = await fetch(`${API_BASE}${path}${qs}`, { signal });
  const json = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg = json?.detail || `GET ${path} failed`;
    throw new Error(msg);
  }
  return json;
}
async function apiPost(path, body) {
  const resp = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const json = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg = json?.detail || `POST ${path} failed`;
    throw new Error(msg);
  }
  return json;
}

// -------------------------------------
// Local, small components
// -------------------------------------
const FieldRow = {
  name: "FieldRow",
  props: {
    label: { type: String, required: true },
    field: { type: String, required: true },
    modelValue: { required: false },
    edit: { type: Boolean, default: false },
    type: { type: String, default: "text" }, // text | textarea | date | time | datetime-local | select-bool
  },
  emits: ["update:modelValue", "update:edit"],
  setup(props, { emit }) {
    const localVal = ref(props.modelValue);
    const inputRef = ref(null);

    watch(
      () => props.modelValue,
      (v) => {
        localVal.value = v;
      }
    );

    watch(
      () => props.edit,
      async (isEdit) => {
        if (isEdit) {
          await nextTick();
          if (inputRef.value) {
            inputRef.value.focus();
            const el = inputRef.value;
            if (el.setSelectionRange) {
              const len = String(el.value || "").length;
              el.setSelectionRange(len, len);
            }
          }
        }
      }
    );

    function onInput(e) {
      emit("update:modelValue", e?.target?.value ?? e);
    }
    function setEdit(v) {
      emit("update:edit", !!v);
    }

    return { localVal, inputRef, onInput, setEdit };
  },
  template: `
    <div class="field-row">
      <div class="field-label">{{ label }}</div>
      <div class="field-value" v-if="!edit">{{ modelValue ?? "-" }}</div>
      <div class="edit-input" v-else>
        <textarea v-if="type==='textarea'" ref="inputRef" class="form-input" :value="localVal" @input="onInput" rows="3"></textarea>
        <template v-else>
          <input v-if="type!=='select-bool'" ref="inputRef" class="form-input" :type="type" :value="localVal" @input="onInput" />
          <select v-else ref="inputRef" class="form-input" :value="localVal" @change="onInput">
            <option :value="true">True</option>
            <option :value="false">False</option>
          </select>
        </template>
      </div>
      <button class="edit-btn" @click="setEdit(!edit)">{{ edit ? 'Done' : 'Edit' }}</button>
    </div>
  `,
};

// -------------------------------------
// Main component
// -------------------------------------
export default {
  name: "MerchantDetail",
  components: { FieldRow, MerchantTwitter },

  setup() {
    // Routing
    const route = useRoute();
    const router = useRouter();

    // Route helpers
    const routeName = computed(() => {
      return (
        (route.params.name && String(route.params.name)) ||
        (route.query.merchant && String(route.query.merchant)) ||
        ""
      );
    });
    const routeId = computed(() => {
      return (
        (route.params.id && String(route.params.id)) ||
        (route.query.id && String(route.query.id)) ||
        ""
      );
    });
    const routeIdentifier = computed(
      () => routeName.value || routeId.value || ""
    );

    // Base state
    const loading = ref(true);
    const saving = ref(false);
    const originalDoc = ref(null);

    // Merchant model (view-model)
    const merchant = reactive({
      id: "",
      name: "",
      url: "",
      language_code: "",
      time_zone_id: "",
      legal_name: "",
      acceptor_name: "",
      acceptor_category_code: "",
      country_code: "",
      country_sub_division_code: "",
      home_country_code: "",
      region_id: "",
      city: "",
      postal_code: "",
      street: "",
      city_category_code: "",
      business_service_phone_number: "",
      customer_service_phone_number: "",
      additional_contact_information: "",
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
      start_date: "",
      end_date: "",
      created_at: "",
      updated_at: "",
      merchant_code: "",
      description: "",
      status: "Unknown",
    });

    // Edit flags for fields
    const editFields = reactive({});
    [
      "merchant_code",
      "url",
      "language_code",
      "time_zone_id",
      "legal_name",
      "acceptor_name",
      "acceptor_category_code",
      "country_code",
      "country_sub_division_code",
      "home_country_code",
      "region_id",
      "city",
      "postal_code",
      "street",
      "city_category_code",
      "business_service_phone_number",
      "customer_service_phone_number",
      "additional_contact_information",
      "currency_code",
      "tax_id",
      "trade_register_number",
      "iban",
      "domiciliary_bank_number",
      "cut_off_time",
      "activation_flag",
      "activation_time",
      "activation_start_time",
      "activation_end_time",
      "start_date",
      "end_date",
      "description",
    ].forEach((k) => (editFields[k] = false));

    // Status and labels
    const isActive = computed(() => !!merchant.activation_flag);
    const fullAddress = computed(() =>
      [merchant.street, merchant.city, merchant.postal_code, merchant.country_code]
        .filter(Boolean)
        .join(", ")
    );

    const mccMap = {
      "5411": "Grocery Stores",
      "5812": "Restaurants",
      "5999": "Misc Retail",
      "5732": "Electronics",
      "4789": "Transportation",
      "7399": "Business Services",
      "4814": "Telecom",
      "5699": "Apparel",
      "4900": "Utilities",
      "5921": "Liquor Stores",
      "5813": "Bars/Clubs",
      "6051": "Prepaid",
      "6011": "ATMs",
      "7995": "Gambling",
    };
    const mccLabel = computed(() => {
      const code = merchant.acceptor_category_code;
      return code && mccMap[code] ? `(${mccMap[code]})` : "";
    });

    // Actions and modals (kept compact)
    const actionsTaken = ref([]);
    const showPermanentBanModal = ref(false);
    const showShadowBanModal = ref(false);
    const showRestrictModal = ref(false);
    function openPermanentBanModal() {
      showPermanentBanModal.value = true;
    }
    function closePermanentBanModal() {
      showPermanentBanModal.value = false;
    }
    function openShadowBanModal() {
      showShadowBanModal.value = true;
    }
    function closeShadowBanModal() {
      showShadowBanModal.value = false;
    }
    function openRestrictModal() {
      showRestrictModal.value = true;
    }
    function closeRestrictModal() {
      showRestrictModal.value = false;
    }
    function confirmPermanentBan() {
      actionsTaken.value.push("Permanent Ban");
      showPermanentBanModal.value = false;
      toast.error(`${merchant.name} has been permanently banned!`, {
        autoClose: 5000,
        position: "top-right",
      });
    }
    function confirmShadowBan() {
      actionsTaken.value.push("Shadow Ban");
      showShadowBanModal.value = false;
      toast.warning(`${merchant.name} has been shadow banned!`, {
        autoClose: 5000,
        position: "top-right",
      });
    }
    // Simple restrictions model
    const restrictionData = reactive({
      dailyTransactionLimit: null,
      monthlyTransactionLimit: null,
      maxTransactionAmount: null,
      restrictedCategoryCodes: [],
      allowedCountries: "",
      requireAdditionalVerification: false,
      blockInternationalTransactions: false,
      reason: "",
      startDate: new Date().toISOString().split("T")[0],
      endDate: "",
    });
    const selectedCategoryCode = ref("");
    function addCategoryCode() {
      const code = (selectedCategoryCode.value || "").trim();
      if (!code) return;
      if (!restrictionData.restrictedCategoryCodes.includes(code)) {
        restrictionData.restrictedCategoryCodes.push(code);
      }
      selectedCategoryCode.value = "";
    }
    function removeCategoryCode(code) {
      const ix = restrictionData.restrictedCategoryCodes.indexOf(code);
      if (ix >= 0) restrictionData.restrictedCategoryCodes.splice(ix, 1);
    }
    function confirmRestriction() {
      if (!restrictionData.reason.trim()) {
        toast.error("Please provide a reason for the restriction.", {
          autoClose: 3000,
          position: "top-right",
        });
        return;
      }
      actionsTaken.value.push("Restricted");
      showRestrictModal.value = false;
      toast.success(`Restrictions applied on ${merchant.name}!`, {
        autoClose: 4000,
        position: "top-right",
      });
    }

    // Auto-refresh controls
    const autoRefresh = ref(false);
    const refreshSec = ref(10);
    let refreshTimer = null;

    watch(autoRefresh, (on) => {
      if (refreshTimer) {
        clearInterval(refreshTimer);
        refreshTimer = null;
      }
      if (on) {
        refreshTimer = setInterval(() => {
          // Only refresh streams by default; full doc reload is heavier.
          fetchStreams();
        }, Math.max(2000, (refreshSec.value || 10) * 1000));
      }
    });
    watch(refreshSec, () => {
      if (autoRefresh.value) {
        autoRefresh.value = false;
        autoRefresh.value = true; // retrigger timer with new interval
      }
    });

    // -------------------------------------
    // Merchant fetch + mapping
    // -------------------------------------
    function mapDocToView(doc) {
      originalDoc.value = doc;

      merchant.id = doc.merchant_id || "";
      merchant.name = doc.merchant_name || "";

      merchant.merchant_code = doc.merchant_code || "";
      merchant.url = doc.url || "";
      merchant.language_code = doc.language_code || "";
      merchant.time_zone_id = doc.time_zone_id || "";

      merchant.legal_name = doc.legal_name || "";
      merchant.acceptor_name = doc.acceptor_name || "";
      merchant.acceptor_category_code = doc.acceptor_category_code || "";

      merchant.country_code = doc.country_code || "";
      merchant.country_sub_division_code = doc.country_sub_division_code || "";
      merchant.home_country_code = doc.home_country_code || "";
      merchant.region_id = doc.region_id || "";
      merchant.city = doc.city || "";
      merchant.postal_code = doc.postal_code || "";
      merchant.street = doc.street || "";
      merchant.city_category_code = doc.city_category_code || "";

      merchant.business_service_phone_number =
        doc.business_service_phone_number || "";
      merchant.customer_service_phone_number =
        doc.customer_service_phone_number || "";
      merchant.additional_contact_information =
        doc.additional_contact_information || "";

      merchant.currency_code = doc.currency_code || "";
      merchant.tax_id = doc.tax_id || "";
      merchant.trade_register_number = doc.trade_register_number || "";
      merchant.iban = (doc.iban === null ? "" : doc.iban) || "";
      merchant.domiciliary_bank_number = doc.domiciliary_bank_number || "";
      merchant.cut_off_time = (doc.cut_off_time || "").slice(0, 5); // HH:MM

      merchant.activation_flag = !!doc.activation_flag;
      merchant.activation_time = isoToLocal(doc.activation_time || "");
      merchant.activation_start_time = isoToLocal(
        doc.activation_start_time || ""
      );
      merchant.activation_end_time = isoToLocal(doc.activation_end_time || "");

      merchant.start_date = doc.start_date || "";
      merchant.end_date = doc.end_date || "";
      merchant.created_at = doc.created_at || "";
      merchant.updated_at = doc.updated_at || "";

      merchant.description = doc.description || "";

      merchant.status = merchant.activation_flag ? "Active" : "Inactive";

      Object.keys(editFields).forEach((k) => (editFields[k] = false));
      loading.value = false;
    }

    function mapViewToDetails() {
      // Prepare a narrow "details" object compatible with backend "onboard" details
      return {
        merchant_code: merchant.merchant_code || undefined,
        url: merchant.url || undefined,
        language_code: merchant.language_code || undefined,
        time_zone_id: merchant.time_zone_id || undefined,

        legal_name: merchant.legal_name || undefined,
        acceptor_name: merchant.acceptor_name || undefined,
        acceptor_category_code: merchant.acceptor_category_code || undefined,

        country_code: merchant.country_code || undefined,
        country_sub_division_code: merchant.country_sub_division_code || undefined,
        home_country_code: merchant.home_country_code || undefined,
        region_id: merchant.region_id || undefined,
        city: merchant.city || undefined,
        postal_code: merchant.postal_code || undefined,
        street: merchant.street || undefined,
        city_category_code: merchant.city_category_code || undefined,

        business_service_phone_number:
          merchant.business_service_phone_number || undefined,
        customer_service_phone_number:
          merchant.customer_service_phone_number || undefined,
        additional_contact_information:
          merchant.additional_contact_information || undefined,

        currency_code: merchant.currency_code || undefined,
        tax_id: merchant.tax_id || undefined,
        trade_register_number: merchant.trade_register_number || undefined,
        iban: merchant.iban === "" ? null : merchant.iban,
        domiciliary_bank_number: merchant.domiciliary_bank_number || undefined,
        cut_off_time: toHHMMSS(merchant.cut_off_time || "") || undefined,

        activation_flag: !!merchant.activation_flag,
        activation_time: merchant.activation_time
          ? new Date(merchant.activation_time).toISOString()
          : null,
        activation_start_time: merchant.activation_start_time
          ? new Date(merchant.activation_start_time).toISOString()
          : null,
        activation_end_time: merchant.activation_end_time
          ? new Date(merchant.activation_end_time).toISOString()
          : null,

        start_date: merchant.start_date || undefined,
        end_date: merchant.end_date || undefined,
        description: merchant.description || undefined,
      };
    }

    async function fetchByName(name) {
      const data = await apiGet(`/v1/merchants/${encodeURIComponent(name)}`);
      const doc = data?.merchant || null;
      if (!doc) throw new Error("Merchant not found");
      mapDocToView(doc);
    }

    async function fetchById(id) {
      // If your backend supports by-id lookup via the same endpoint (service resolves it),
      // this will work. Otherwise, you can adapt to your by-id route.
      const data = await apiGet(`/v1/merchants/${encodeURIComponent(id)}`);
      const doc = data?.merchant || null;
      if (!doc) throw new Error("Merchant not found");
      mapDocToView(doc);
    }

    const loadFromRoute = async () => {
      loading.value = true;
      try {
        const ident = routeIdentifier.value;
        if (!ident) throw new Error("No merchant specified.");
        if (/^acc_[a-f0-9]+$/i.test(ident)) {
          await fetchById(ident);
        } else {
          await fetchByName(ident);
        }
        // Fetch streams after merchant is loaded
        await fetchStreams();
      } catch (e) {
        loading.value = false;
        originalDoc.value = null;
        toast.error(String(e?.message || e), { autoClose: 3000 });
      }
    };

    // Change detection
    const hasChanges = computed(() => {
      const doc = originalDoc.value;
      if (!doc) return false;
      const cmp = (a, b) => (a ?? null) !== (b ?? null);

      if (cmp(merchant.merchant_code, doc.merchant_code)) return true;
      if (cmp(merchant.url, doc.url)) return true;
      if (cmp(merchant.language_code, doc.language_code)) return true;
      if (cmp(merchant.time_zone_id, doc.time_zone_id)) return true;

      if (cmp(merchant.legal_name, doc.legal_name)) return true;
      if (cmp(merchant.acceptor_name, doc.acceptor_name)) return true;
      if (cmp(merchant.acceptor_category_code, doc.acceptor_category_code))
        return true;

      if (cmp(merchant.country_code, doc.country_code)) return true;
      if (
        cmp(merchant.country_sub_division_code, doc.country_sub_division_code)
      )
        return true;
      if (cmp(merchant.home_country_code, doc.home_country_code)) return true;
      if (cmp(merchant.region_id, doc.region_id)) return true;
      if (cmp(merchant.city, doc.city)) return true;
      if (cmp(merchant.postal_code, doc.postal_code)) return true;
      if (cmp(merchant.street, doc.street)) return true;
      if (cmp(merchant.city_category_code, doc.city_category_code)) return true;

      if (
        cmp(
          merchant.business_service_phone_number,
          doc.business_service_phone_number
        )
      )
        return true;
      if (
        cmp(
          merchant.customer_service_phone_number,
          doc.customer_service_phone_number
        )
      )
        return true;
      if (
        cmp(
          merchant.additional_contact_information,
          doc.additional_contact_information
        )
      )
        return true;

      if (cmp(merchant.currency_code, doc.currency_code)) return true;
      if (cmp(merchant.tax_id, doc.tax_id)) return true;
      if (cmp(merchant.trade_register_number, doc.trade_register_number))
        return true;
      const ibanNorm = merchant.iban === "" ? null : merchant.iban;
      if (cmp(ibanNorm, doc.iban ?? null)) return true;
      if (cmp(merchant.domiciliary_bank_number, doc.domiciliary_bank_number))
        return true;
      if (cmp(toHHMMSS(merchant.cut_off_time || ""), doc.cut_off_time))
        return true;

      if (cmp(!!merchant.activation_flag, !!doc.activation_flag)) return true;
      const actTimeISO = merchant.activation_time
        ? new Date(merchant.activation_time).toISOString()
        : null;
      const actStartISO = merchant.activation_start_time
        ? new Date(merchant.activation_start_time).toISOString()
        : null;
      const actEndISO = merchant.activation_end_time
        ? new Date(merchant.activation_end_time).toISOString()
        : null;
      if (cmp(actTimeISO, doc.activation_time || null)) return true;
      if (cmp(actStartISO, doc.activation_start_time || null)) return true;
      if (cmp(actEndISO, doc.activation_end_time || null)) return true;

      if (cmp(merchant.start_date, doc.start_date)) return true;
      if (cmp(merchant.end_date, doc.end_date)) return true;
      if (cmp(merchant.description, doc.description)) return true;

      return false;
    });

    // Save updates via /v1/onboard (background task)
    async function saveUpdates() {
      if (!originalDoc.value || !merchant.name) return;
      saving.value = true;
      try {
        const details = mapViewToDetails();
        const body = {
          merchant_name: merchant.name,
          deep_scan: false,
          details,
          preset_overrides: {},
          start_date: merchant.start_date || undefined,
          end_date: merchant.end_date || undefined,
          seed: undefined,
        };
        const res = await apiPost("/v1/onboard", body);
        const tid = res?.task_id;
        if (!tid) {
          toast.success("Saved.");
          await loadFromRoute();
          return;
        }
        // Poll task until done
        const started = Date.now();
        let status = "pending";
        while (status === "pending" || status === "running") {
          await new Promise((r) => setTimeout(r, 600));
          const t = await apiGet(`/v1/tasks/${encodeURIComponent(tid)}`);
          status = t?.status || "unknown";
          if (status === "done") break;
          if (status === "error") throw new Error(t?.error || "Task failed");
          if (Date.now() - started > 30_000) break; // 30s sanity timeout
        }
        toast.success("Saved and refreshed.");
        await loadFromRoute();
      } catch (e) {
        toast.error(String(e?.message || e), { autoClose: 4000 });
      } finally {
        saving.value = false;
      }
    }

    // Navigation helpers
    function goBack() {
      router.back();
    }
    function openEmailBox() {
      toast.info("Open notifications center", { autoClose: 2000 });
    }
    function openSocialUpdates() {
      toast.info("Open social updates", { autoClose: 2000 });
    }
    function openExplore() {
      toast.info("Opening explore", { autoClose: 2000 });
      router.push(`/explore/${this.merchant.name}`)
    }

    // -------------------------------------
    // Streams: Tweets only (efficient)
    // -------------------------------------
    const tweets = ref([]);
    const tweetsLoading = ref(false);
    const tweetsError = ref("");

    // Query params (aligned with backend)
    const streamWindow = ref("7d"); // e.g., 1h, 6h, 1d, 7d
    const streamOrder = ref("desc");
    const streamLimit = ref(5000);
    const allowFuture = ref(false);
    const sinceStr = ref(""); // optional ISO/epoch
    const untilStr = ref(""); // optional ISO/epoch
    const nowIso = ref(""); // optional; let server use real now if blank

    // Client-side aggregation granularity for child charts
    const unit = ref("hour");

    const merchantKey = computed(() => {
      // Prefer the merchant_name from the loaded doc
      return (
        (originalDoc.value && originalDoc.value.merchant_name) ||
        (routeName.value || "").trim() ||
        (routeIdentifier.value || "").trim()
      );
    });

    let streamsAbort = null;

    async function fetchStreams() {
      if (!merchantKey.value) return;
      if (streamsAbort) streamsAbort.abort();
      streamsAbort = new AbortController();

      tweetsLoading.value = true;
      tweetsError.value = "";

      try {
        const params = new URLSearchParams();
        params.set("streams", "tweets"); // only tweets for efficiency
        params.set("order", streamOrder.value || "desc");
        if (streamWindow.value && streamWindow.value.trim())
          params.set("window", streamWindow.value.trim());
        if ((sinceStr.value || "").trim()) params.set("since", sinceStr.value.trim());
        if ((untilStr.value || "").trim()) params.set("until", untilStr.value.trim());
        if (Number(streamLimit.value) > 0)
          params.set("limit", String(Number(streamLimit.value)));
        if (allowFuture.value) params.set("allow_future", "true");
        if ((nowIso.value || "").trim()) params.set("now", nowIso.value.trim());

        const url = `/v1/${encodeURIComponent(merchantKey.value)}/data`;
        const json = await apiGet(url, Object.fromEntries(params), streamsAbort.signal);

        tweets.value = Array.isArray(json?.data?.tweets) ? json.data.tweets : [];
      } catch (e) {
        if (String(e?.name) !== "AbortError") {
          tweetsError.value = String(e?.message || e);
          tweets.value = [];
          toast.error(tweetsError.value, { autoClose: 3000 });
        }
      } finally {
        tweetsLoading.value = false;
      }
    }

    // Refetch when any stream control changes
    watch(
      [streamWindow, streamOrder, streamLimit, allowFuture, sinceStr, untilStr, nowIso],
      () => {
        fetchStreams();
      }
    );

    // On merchant change (route) refetch
    watch(merchantKey, () => {
      fetchStreams();
    });

    // Lifecycle
    onMounted(loadFromRoute);
    watch(
      () => route.fullPath,
      () => {
        loadFromRoute();
      }
    );

    // Expose to template
    return {
      // route
      routeIdentifier,
      // base
      merchant,
      originalDoc,
      editFields,
      loading,
      saving,
      hasChanges,
      isActive,
      fullAddress,
      mccLabel,
      // actions
      actionsTaken,
      showPermanentBanModal,
      showShadowBanModal,
      showRestrictModal,
      openPermanentBanModal,
      closePermanentBanModal,
      openShadowBanModal,
      closeShadowBanModal,
      openRestrictModal,
      closeRestrictModal,
      confirmPermanentBan,
      confirmShadowBan,
      confirmRestriction,
      selectedCategoryCode,
      restrictionData,
      addCategoryCode,
      removeCategoryCode,
      // utils
      formatDate,
      // ops
      saveUpdates,
      goBack,
      openEmailBox,
      openSocialUpdates,
      openExplore,
      // auto refresh
      autoRefresh,
      refreshSec,
      // streams
      tweets,
      tweetsLoading,
      tweetsError,
      streamWindow,
      streamOrder,
      streamLimit,
      allowFuture,
      sinceStr,
      untilStr,
      nowIso,
      unit,
      fetchStreams,
    };
  },
};
</script>


<style scoped>
/* Layout and overall styles */
.merchant-detail-container {
  margin: 0 auto;
  padding: 20px;
  font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
  background-color: #f5f5f5;
  min-height: 100vh;
}

.layout {
  display: grid;
  grid-template-columns: 320px 1fr;
  gap: 20px;
}

/* Breadcrumb */
.page-header {
  padding: 16px 20px;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
}
.breadcrumb {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 14px;
  color: #6b7280;
}
.breadcrumb-item { color: #6b7280; text-decoration: none; transition: color 0.2s ease; }
.breadcrumb-item:hover { color: #008080; }
.breadcrumb-item.current { color: #008080; font-weight: 500; }
.breadcrumb-separator { color: #d1d5db; user-select: none; }

/* Side panel */
.side-panel {
    background: #ffffff;
    border-radius: 12px;
    box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    border: 1px solid #e5e7eb;
    padding: 18px;
    height: -moz-fit-content;
    height: fit-content;
    height: -webkit-fill-available;
    height: 81vh;
    position: sticky;
    top: 20px;
    display: flex
;
    flex-direction: column;
    flex-wrap: nowrap;
    align-content: center;
    justify-content: space-around;
    align-items: stretch;
}

.merchant-header {
  background: linear-gradient(135deg, #008080, #20b2aa);
  color: white;
  padding: 16px;
  border-radius: 10px;
  box-shadow: 0 4px 12px rgba(0, 128, 128, 0.2);
  margin-bottom: 14px;
}
.title-row { display: flex; align-items: center; gap: 10px; }
.merchant-title { font-size: 18px; font-weight: 600; margin: 0; }
.merchant-id {
  margin-top: 6px;
  background: rgba(255, 255, 255, 0.2);
  padding: 4px 10px;
  border-radius: 5px;
  font-size: 0.85em; font-weight: 500; display: inline-block;
}
.status-dot { width: 10px; height: 10px; border-radius: 50%; border: 1px solid rgba(255,255,255,0.7); }
.status-dot.active { background: #34d399; }
.status-dot.inactive { background: #ef4444; }

.summary-list { margin-top: 12px; display: grid; gap: 8px; }
.summary-item { display: flex; justify-content: space-between; align-items: center; }
.summary-item .label { color: #6b7280; font-size: 12px; }
.summary-item .value { color: #111827; font-weight: 600; font-size: 13px; }

.panel-section { margin-top: 16px; }
.panel-actions { display: grid; gap: 10px; margin-top: 10px; }
.notification {
  background: #f0fdfa; color: #008080; border: 1px solid #14b8a6; padding: 10px 12px; border-radius: 6px;
  font-size: 14px; font-weight: 600; text-align: center; box-shadow: 0 2px 4px rgba(20, 184, 166, 0.1); cursor: pointer;
}
.notification:hover { background: #008080; color: #f0fdfa; }

.action-center-dropdown { display: flex; flex-direction: column; gap: 8px; }
.action-center-dropdown button {
  padding: 10px 16px; border: 1px solid #14b8a6; border-radius: 6px; background: #ffffff; color: #008080;
  font: 13px "Segoe UI", sans-serif; font-weight: 500; cursor: pointer; transition: all 0.2s ease; width: 100%;
}
.action-center-dropdown button:hover {
  background: #008080; color: #ffffff; transform: translateY(-1px); box-shadow: 0 2px 6px rgba(0, 128, 128, 0.2);
}
.action-center-dropdown button:active { transform: translateY(0); }

.primary-btn{
  background: #008080 !important; color: white !important;
  padding: 1rem !important;
}
.primary-btn:hover {
  background: #006666 !important;
}

.actions-taken { text-align: center; color: #666; font-size: 14px; margin-top: 12px; }
.actions-taken-data { color: #999; font-size: 13px; font-style: italic; }
.actions-taken h1 { text-align: center; color: #008080; font-size: 13px; font-style: italic; }

/* Buttons */
.btn { padding: 10px 14px; border-radius: 6px; font-weight: 600; cursor: pointer; transition: all 0.2s ease; border: 1px solid transparent; }
.btn-primary { background: #008080; color: white; }
.btn-primary:hover { background: #006666; }
.btn-primary:disabled { opacity: 0.2; cursor: not-allowed; background-color: #4e4e4e91 !important; border: transparent; }
.btn-secondary { background: white; color: #008080; border-color: #14b8a6; }
.btn-secondary:hover { background: #f0fdfa; }

/* Main content */
.merchant-detail { display: grid; gap: 20px; }

.card {
  background: white; border-radius: 12px; box-shadow: 0 3px 12px rgba(0, 0, 0, 0.08);
  border: 1px solid #e5e7eb; overflow: hidden;
}
.card-header { padding: 16px 18px; background: #f8fafc; border-bottom: 1px solid #e5e7eb; }
.card-header h3 { margin: 0; color: #008080; font-size: 16px; font-weight: 600; }
.card-body { padding: 16px 18px; display: grid; gap: 12px; }
.card-footer { padding: 12px 18px; background: #fafbfc; border-top: 1px solid #e5e7eb; }
.meta-footer { display: flex; gap: 24px; color: #374151; font-size: 13px; }
.grid-2 { grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }

/* Finance badges */
.finance-badges { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 8px; margin-bottom: 6px; }
.finance-badges div { font-size: 13px; color: #374151; }
.finance-badges strong { color: #111827; }

/* FieldRow styles */
.field-row {
  display: grid; grid-template-columns: 200px 1fr auto; align-items: center; gap: 12px;
}
.field-label { color: #374151; font-weight: 600; font-size: 13px; }
.field-value { color: #111827; font-size: 14px; }
.edit-btn {
  background: #f0fdfa; color: #008080; border: 1px solid #14b8a6;
  padding: 6px 10px; border-radius: 6px; cursor: pointer; font-size: 12px;
}
.edit-btn:hover { background: #008080; color: #f0fdfa; }
.edit-input { display: grid; grid-template-columns: 1fr; gap: 8px; margin-top: 6px; }

.form-input, .form-textarea, select.form-input {
  width: 100%; padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 14px; transition: all 0.2s ease; box-sizing: border-box;
}
.form-textarea { resize: vertical; min-height: 80px; }

/* Description row */
.desc-row { display: grid; grid-template-columns: 200px 1fr auto; align-items: start; gap: 12px; }
.desc-label { color: #374151; font-weight: 600; font-size: 13px; }
.desc-content { display: grid; gap: 8px; }
.desc-text { color: #111827; font-size: 14px; }
.desc-edit { }

/* Display rows */
.row-display { margin: 0; font-size: 14px; color: #374151; display: flex; gap: 8px; }
.row-display strong { color: #111827; }

/* Modal styles */
.modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0, 0, 0, 0.6); display: flex; justify-content: center; align-items: center; z-index: 1000; backdrop-filter: blur(4px); }
.modal-content { background: white; border-radius: 12px; box-shadow: 0 20px 60px rgba(0,0,0,0.3); max-width: 500px; width: 90%; max-height: 90vh; overflow-y: auto; animation: modalSlideIn 0.3s ease; }
.modal-content.large { max-width: 800px; }
@keyframes modalSlideIn { from { opacity: 0; transform: translateY(-50px) scale(0.9); } to { opacity: 1; transform: translateY(0) scale(1); } }
.modal-header { padding: 20px 24px; border-bottom: 1px solid #e5e7eb; display: flex; justify-content: space-between; align-items: center; background: linear-gradient(135deg, #f8fafc, #f1f5f9); }
.modal-header h3 { margin: 0; color: #008080; font-size: 18px; font-weight: 600; }
.close-btn { background: none; border: none; font-size: 24px; color: #9ca3af; cursor: pointer; padding: 0; width: 30px; height: 30px; display: flex; align-items: center; justify-content: center; border-radius: 50%; transition: all 0.2s ease; }
.close-btn:hover { background: #f3f4f6; color: #374151; }
.modal-body { padding: 24px; }
.warning-icon { font-size: 48px; text-align: center; margin-bottom: 16px; }
.warning-text { font-size: 16px; text-align: center; margin-bottom: 12px; color: #374151; }
.warning-subtext { font-size: 14px; text-align: center; color: #6b7280; margin-bottom: 0; }
.modal-footer { padding: 16px 24px; border-top: 1px solid #e5e7eb; display: flex; justify-content: flex-end; gap: 12px; background: #fafbfc; }

/* Buttons in modal */
.btn-cancel { padding: 10px 20px; border: 1px solid #d1d5db; border-radius: 6px; background: white; color: #374151; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-cancel:hover { background: #f9fafb; border-color: #9ca3af; }
.btn-danger { padding: 10px 20px; border: none; border-radius: 6px; background: #dc2626; color: white; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-danger:hover { background: #b91c1c; transform: translateY(-1px); }
.btn-warning { padding: 10px 20px; border: none; border-radius: 6px; background: #f59e0b; color: white; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-warning:hover { background: #d97706; transform: translateY(-1px); }
.btn-primary { padding: 10px 20px; border: none; border-radius: 6px; background: #008080; color: white; font-weight: 500; cursor: pointer; transition: all 0.2s ease; }
.btn-primary:disabled { opacity: 0.6; cursor: not-allowed; }
.btn-primary:hover { background: #006666; transform: translateY(-1px); }

/* Loading and error */
.loading, .error {
  text-align: center; padding: 40px; color: #666; background: white; border-radius: 12px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.08);
}

@media (max-width: 1024px) {
  .layout { grid-template-columns: 1fr; }
  .side-panel { position: static; }
}
@media (max-width: 768px) {
  .merchant-detail-container { padding: 15px; }
  .card-body { padding: 14px; }
  .card-header { padding: 14px; }
  .field-row, .desc-row { grid-template-columns: 1fr; }
}
@media (max-width: 480px) {
  .merchant-detail-container { padding: 10px; }
  .card-body { padding: 12px; }
  .card-header { padding: 12px; }
}

.meta-label{
  color: #6b7280;
  font-weight: 900;
}
.read-only{
  background: #f9fafb;
  cursor: not-allowed;
}

.meta-item{
  background: #f9fafb;
  border: 1px solid #e5e7eb67;
  border-radius: 6px;
  /* padding: 4px; */
}
.meta-item:hover{
  /* border-color: #9ca3af; */
  background: #f3f4f6;
}
</style>