<template>
  <div class="field">
    <label>{{ label }}</label>
    <div class="value">
      <template v-if="edit[field]">
        <textarea v-if="type==='textarea'" v-model="editable[field]" class="form-input" rows="3"></textarea>
        <input v-else-if="type==='date'" type="date" v-model="editable[field]" class="form-input" />
        <input v-else-if="type==='time'" type="time" v-model="editable[field]" class="form-input" />
        <input v-else-if="type==='datetime-local'" type="datetime-local" v-model="editable[field]" class="form-input" />
        <select v-else-if="type==='select-bool'" v-model="editable[field]" class="form-input">
          <option :value="true">Enabled</option>
          <option :value="false">Disabled</option>
        </select>
        <input v-else v-model="editable[field]" class="form-input" />
      </template>
      <template v-else>
        <span>{{ editable[field] || 'N/A' }}</span>
      </template>
      <i class="edit-icon" @click="toggle">✎</i>
    </div>
  </div>
</template>

<script>
export default {
  name: 'Field',
  props: {
    label: String,
    field: String,
    editable: Object,
    edit: Object,
    type: { type: String, default: 'text' }
  },
  emits: ['toggle'],
  setup(props, { emit }) {
    const toggle = () => emit('toggle', props.field);
    return { toggle };
  }
};
</script>

<style scoped>
.field { }
</style>
