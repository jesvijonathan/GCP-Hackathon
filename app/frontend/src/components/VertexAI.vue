<template>
  <!-- AI Chat Button -->
  <button class="vertex-ai-btn" @click="openChatModal">
    <span class="btn-icon">🤖</span>
    <span class="btn-text">AI Assistant</span>
  </button>

  <!-- Chat Modal -->
  <div v-if="showChatModal" class="modal-overlay" @click="closeChatModal">
    <div class="modal-content chat-modal" @click.stop>
      <!-- Modal Header -->
      <div class="modal-header">
        <div class="header-content">
          <div class="ai-avatar">🤖</div>
          <div class="header-text">
            <h2 class="modal-title">AI Assistant</h2>
            <p class="modal-subtitle">Powered by Vertex AI</p>
          </div>
        </div>
        <button class="close-btn" @click="closeChatModal" title="Close">
          ×
        </button>
      </div>

      <!-- Chat Messages Area -->
      <div class="modal-body" ref="chatContainer">
        <!-- Initial Loading State -->
        <div v-if="isInitializing" class="initial-loading">
          <div class="loading-spinner">
            <div class="spinner-ring"></div>
            <div class="spinner-icon">🤖</div>
          </div>
          <p class="loading-text">Analyzing your data...</p>
        </div>

        <!-- Chat Messages -->
        <div v-else class="chat-messages">
          <div
            v-for="(message, index) in chatHistory"
            :key="index"
            class="message-wrapper"
            :class="message.role"
          >
            <div class="message-bubble" :class="message.role">
              <div class="message-header">
                <span class="message-avatar">{{
                  message.role === "user" ? "👤" : "🤖"
                }}</span>
                <span class="message-role">{{
                  message.role === "user" ? "You" : "AI Assistant"
                }}</span>
                <span class="message-time">{{ message.timestamp }}</span>
              </div>
              <div
                class="message-content"
                v-html="formatMessage(message.content)"
              ></div>
            </div>
          </div>

          <!-- Typing Indicator -->
          <div v-if="isTyping" class="message-wrapper assistant">
            <div class="message-bubble assistant typing-indicator">
              <div class="message-header">
                <span class="message-avatar">🤖</span>
                <span class="message-role">AI Assistant</span>
              </div>
              <div class="typing-dots">
                <div class="dot"></div>
                <div class="dot"></div>
                <div class="dot"></div>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Input Area -->
      <div class="modal-footer" v-if="!isInitializing">
        <div class="input-container">
          <textarea
            v-model="userQuestion"
            @keydown.enter.prevent="handleEnterKey"
            placeholder="Follow-up question..."
            class="question-input"
            rows="1"
            ref="questionInput"
          ></textarea>
          <button
            class="ask-btn"
            @click="askFollowUpQuestion"
            :disabled="!userQuestion.trim() || isTyping"
            :class="{ disabled: !userQuestion.trim() || isTyping }"
          >
            <span v-if="!isTyping">ASK ➤</span>
            <span v-else>
              <div class="btn-spinner"></div>
            </span>
          </button>
        </div>
        <!-- <div class="input-hint">
          Press <kbd>Enter</kbd> to send • <kbd>Shift + Enter</kbd> for new line
        </div> -->
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: "VertexAI",
  props: {
    socialData: {
      type: Object,
      required: true,
    },
    merchantName: {
      type: String,
      default: "Merchant",
    },
  },
  data() {
    return {
      showChatModal: false,
      chatHistory: [],
      userQuestion: "",
      isTyping: false,
      isInitializing: false,
      // REPLACE WITH YOUR ACTUAL API KEY
      apiKey: "AQ.Ab8RN6IDHpFwvn2fIB_zN5jYZlBkiASFWlEL82Xg-ix2Yepn0A",
      apiEndpoint: "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-2.5-flash-lite:streamGenerateContent",
    };
  },
  methods: {
    async openChatModal() {
      this.showChatModal = true;
      this.isInitializing = true;
      document.body.style.overflow = "hidden";

      await this.generateInitialSummary();
    },

    closeChatModal() {
      this.showChatModal = false;
      this.chatHistory = [];
      this.userQuestion = "";
      this.isTyping = false;
      this.isInitializing = false;
      document.body.style.overflow = "auto";
    },

    async generateInitialSummary() {
      try {
        const prompt = `Analyze this social media data for ${this.merchantName} and give a natural summary of the provided data... on the basis of risk and what to consider:

${JSON.stringify(this.socialData, null, 2)}`;

        const response = await this.callGeminiAI(prompt);

        this.chatHistory.push({
          role: "assistant",
          content: response,
          timestamp: this.getCurrentTime(),
        });

        this.isInitializing = false;
        this.$nextTick(() => this.scrollToBottom());
      } catch (error) {
        console.error("Error:", error);
        this.chatHistory.push({
          role: "assistant",
          content: `❌ Error: ${error.message}`,
          timestamp: this.getCurrentTime(),
        });
        this.isInitializing = false;
      }
    },

    async askFollowUpQuestion() {
      if (!this.userQuestion.trim() || this.isTyping) return;

      const question = this.userQuestion.trim();
      this.userQuestion = "";

      this.chatHistory.push({
        role: "user",
        content: question,
        timestamp: this.getCurrentTime(),
      });

      this.$nextTick(() => this.scrollToBottom());
      this.isTyping = true;

      try {
        const conversationHistory = this.chatHistory
          .slice(0, -1)
          .map((msg) => `${msg.role === "user" ? "User" : "Assistant"}: ${msg.content}`)
          .join("\n\n");

        const prompt = `Based on this social media data for ${this.merchantName}:

${JSON.stringify(this.socialData, null, 2)}

Previous conversation:
${conversationHistory}

Question: ${question}

Provide a clear, concise answer.`;

        const response = await this.callGeminiAI(prompt);

        this.chatHistory.push({
          role: "assistant",
          content: response,
          timestamp: this.getCurrentTime(),
        });

        this.isTyping = false;
        this.$nextTick(() => this.scrollToBottom());
      } catch (error) {
        console.error("Error:", error);
        this.chatHistory.push({
          role: "assistant",
          content: `❌ Error: ${error.message}`,
          timestamp: this.getCurrentTime(),
        });
        this.isTyping = false;
      }
    },

    async callGeminiAI(prompt) {
      console.log("🔍 Calling Gemini Streaming API...");

      const url = `${this.apiEndpoint}?key=${this.apiKey}`;

      const payload = {
        contents: [
          {
            role: "user",
            parts: [
              {
                text: prompt,
              },
            ],
          },
        ],
        generationConfig: {
          temperature: 0.7,
          topK: 40,
          topP: 0.95,
          maxOutputTokens: 2048,
        },
      };

      try {
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(payload),
        });

        console.log("📥 Response status:", response.status);

        if (!response.ok) {
          const errorText = await response.text();
          console.error("❌ Error response:", errorText);
          throw new Error(`API Error (${response.status}): ${errorText}`);
        }

        const responseText = await response.text();
        console.log("📥 Raw response:", responseText);

        // Parse the streaming response (it's a JSON array)
        let data;
        try {
          data = JSON.parse(responseText);
        } catch (e) {
          console.error("Failed to parse response:", e);
          throw new Error("Invalid response format from API");
        }

        console.log("✅ Parsed response:", data);

        // Combine all streaming chunks into one text
        let fullText = "";

        if (Array.isArray(data)) {
          // It's an array of chunks (streaming response)
          for (const chunk of data) {
            if (chunk?.candidates && chunk.candidates.length > 0) {
              const candidate = chunk.candidates[0];
              if (candidate?.content?.parts && candidate.content.parts.length > 0) {
                const chunkText = candidate.content.parts[0].text || "";
                fullText += chunkText;
              }
            }
          }
        } else if (data?.candidates && data.candidates.length > 0) {
          // It's a single response
          const candidate = data.candidates[0];
          if (candidate?.content?.parts && candidate.content.parts.length > 0) {
            fullText = candidate.content.parts[0].text || "";
          }
        }

        if (!fullText) {
          console.error("⚠️ No text extracted from response");
          throw new Error("No text content in API response");
        }

        console.log("✅ Extracted text:", fullText);
        return fullText;

      } catch (error) {
        console.error("❌ Full error:", error);

        if (error.message.includes("Failed to fetch")) {
          throw new Error("Network error. Check your internet connection.");
        }

        throw error;
      }
    },

    formatMessage(content) {
      let formatted = content
        .replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>")
        .replace(/^### (.*$)/gim, "<h3>$1</h3>")
        .replace(/^## (.*$)/gim, "<h2>$1</h2>")
        .replace(/^# (.*$)/gim, "<h1>$1</h1>")
        .replace(/^\* (.*$)/gim, "<li>$1</li>")
        .replace(/^- (.*$)/gim, "<li>$1</li>")
        .replace(/\n/g, "<br>");

      formatted = formatted.replace(/(<li>.*<\/li>)/gis, "<ul>$1</ul>");
      return formatted;
    },

    getCurrentTime() {
      const now = new Date();
      return now.toLocaleTimeString("en-US", {
        hour: "2-digit",
        minute: "2-digit",
      });
    },

    scrollToBottom() {
      if (this.$refs.chatContainer) {
        this.$refs.chatContainer.scrollTop = this.$refs.chatContainer.scrollHeight;
      }
    },

    handleEnterKey(event) {
      if (event.shiftKey) {
        return;
      } else {
        event.preventDefault();
        this.askFollowUpQuestion();
      }
    },
  },
  beforeUnmount() {
    document.body.style.overflow = "auto";
  },
};
</script>

<style scoped>
/* Vertex AI Button - Teal, White, Grey Theme */
.vertex-ai-btn {
  background: linear-gradient(135deg, #14b8a6, #0d9488);
  color: white;
  border: none;
  padding: 0.75rem 1.5rem;
  border-radius: 10px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.3s ease;
  box-shadow: 0 4px 12px rgba(20, 184, 166, 0.25);
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.95rem;
}

.vertex-ai-btn:hover {
  background: linear-gradient(135deg, #0d9488, #0f766e);
  transform: translateY(-2px);
  box-shadow: 0 6px 16px rgba(20, 184, 166, 0.35);
}

.vertex-ai-btn:active {
  transform: translateY(0);
}

.btn-icon {
  font-size: 1.25rem;
  animation: pulse 2s ease-in-out infinite;
}

@keyframes pulse {
  0%,
  100% {
    transform: scale(1);
  }
  50% {
    transform: scale(1.1);
  }
}

.btn-text {
  font-weight: 600;
}

/* Modal Overlay */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.6);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  padding: 1rem;
  animation: fadeIn 0.2s ease;
}

@keyframes fadeIn {
  from {
    opacity: 0;
  }
  to {
    opacity: 1;
  }
}

/* Chat Modal */
.chat-modal {
  background: #ffffff;
  border-radius: 16px;
  max-width: 900px;
  width: 100%;
  height: 85vh;
  max-height: 800px;
  overflow: hidden;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
  display: flex;
  flex-direction: column;
  animation: slideUp 0.3s ease;
}

@keyframes slideUp {
  from {
    transform: translateY(20px);
    opacity: 0;
  }
  to {
    transform: translateY(0);
    opacity: 1;
  }
}

/* Modal Header */
.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1.25rem 1.5rem;
  background: linear-gradient(135deg, #14b8a6, #0d9488);
  color: white;
  border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.header-content {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.ai-avatar {
  width: 48px;
  height: 48px;
  background: rgba(255, 255, 255, 0.2);
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 1.5rem;
  backdrop-filter: blur(10px);
  border: 2px solid rgba(255, 255, 255, 0.3);
}

.header-text {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.modal-title {
  font-size: 1.25rem;
  font-weight: 700;
  margin: 0;
  color: white;
}

.modal-subtitle {
  font-size: 0.875rem;
  margin: 0;
  color: rgba(255, 255, 255, 0.9);
  font-weight: 400;
}

.close-btn {
  background: rgba(255, 255, 255, 0.15);
  border: none;
  width: 40px;
  height: 40px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  color: white;
  font-size: 1.75rem;
  transition: all 0.2s;
  backdrop-filter: blur(10px);
}

.close-btn:hover {
  background: rgba(255, 255, 255, 0.25);
  transform: rotate(90deg);
}

/* Modal Body - Chat Area */
.modal-body {
  flex: 1;
  overflow-y: auto;
  padding: 1.5rem;
  background: linear-gradient(to bottom, #f8fafc, #f1f5f9);
  scroll-behavior: smooth;
}

/* Scrollbar Styling */
.modal-body::-webkit-scrollbar {
  width: 8px;
}

.modal-body::-webkit-scrollbar-track {
  background: #e5e7eb;
  border-radius: 4px;
}

.modal-body::-webkit-scrollbar-thumb {
  background: #14b8a6;
  border-radius: 4px;
}

.modal-body::-webkit-scrollbar-thumb:hover {
  background: #0d9488;
}

/* Initial Loading State */
.initial-loading {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  gap: 1.5rem;
}

.loading-spinner {
  position: relative;
  width: 80px;
  height: 80px;
}

.spinner-ring {
  position: absolute;
  width: 100%;
  height: 100%;
  border: 4px solid #e5e7eb;
  border-top: 4px solid #14b8a6;
  border-radius: 50%;
  animation: spin 1s linear infinite;
}

@keyframes spin {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}

.spinner-icon {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  font-size: 2rem;
  animation: bounce 1.5s ease-in-out infinite;
}

@keyframes bounce {
  0%,
  100% {
    transform: translate(-50%, -50%) scale(1);
  }
  50% {
    transform: translate(-50%, -55%) scale(1.1);
  }
}

.loading-text {
  color: #6b7280;
  font-size: 1rem;
  font-weight: 500;
}

/* Chat Messages */
.chat-messages {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.message-wrapper {
  display: flex;
  animation: messageSlideIn 0.3s ease;
}

@keyframes messageSlideIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.message-wrapper.user {
  justify-content: flex-end;
}

.message-wrapper.assistant {
  justify-content: flex-start;
}

.message-bubble {
  max-width: 80%;
  padding: 1rem 1.25rem;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
}

.message-bubble.user {
  background: linear-gradient(135deg, #14b8a6, #0d9488);
  color: white;
  border-bottom-right-radius: 4px;
}

.message-bubble.assistant {
  background: white;
  color: #1f2937;
  border: 1px solid #e5e7eb;
  border-bottom-left-radius: 4px;
}

.message-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  font-size: 0.875rem;
}

.message-avatar {
  font-size: 1.25rem;
}

.message-role {
  font-weight: 600;
}

.message-bubble.user .message-role,
.message-bubble.user .message-time {
  color: rgba(255, 255, 255, 0.95);
}

.message-bubble.assistant .message-role,
.message-bubble.assistant .message-time {
  color: #6b7280;
}

.message-time {
  margin-left: auto;
  font-size: 0.75rem;
  opacity: 0.8;
}

.message-content {
  line-height: 1.6;
  font-size: 0.95rem;
}

.message-bubble.user .message-content {
  color: white;
}

.message-bubble.assistant .message-content {
  color: #374151;
}

/* Formatting in messages */
.message-content :deep(h1),
.message-content :deep(h2),
.message-content :deep(h3) {
  margin: 1rem 0 0.5rem 0;
  font-weight: 600;
  color: #14b8a6;
}

.message-content :deep(h1) {
  font-size: 1.25rem;
}
.message-content :deep(h2) {
  font-size: 1.15rem;
}
.message-content :deep(h3) {
  font-size: 1.05rem;
}

.message-content :deep(ul) {
  margin: 0.5rem 0;
  padding-left: 1.5rem;
}

.message-content :deep(li) {
  margin: 0.35rem 0;
}

.message-content :deep(strong) {
  font-weight: 600;
  color: #0d9488;
}

.message-bubble.user .message-content :deep(strong) {
  color: white;
}

/* Typing Indicator */
.typing-indicator {
  background: white;
  border: 1px solid #e5e7eb;
}

.typing-dots {
  display: flex;
  gap: 6px;
  padding: 0.5rem 0;
}

.dot {
  width: 8px;
  height: 8px;
  background: #14b8a6;
  border-radius: 50%;
  animation: dotPulse 1.4s ease-in-out infinite;
}

.dot:nth-child(2) {
  animation-delay: 0.2s;
}

.dot:nth-child(3) {
  animation-delay: 0.4s;
}

@keyframes dotPulse {
  0%,
  60%,
  100% {
    transform: scale(0.8);
    opacity: 0.5;
  }
  30% {
    transform: scale(1.2);
    opacity: 1;
  }
}

/* Modal Footer - Input Area - UPDATED */
.modal-footer {
  padding: 10px;
  background: white;
  border-top: 2px solid #e5e7eb;
  box-shadow: 0 -2px 10px rgba(0, 0, 0, 0.05);
}

.input-container {
  display: flex;
  gap: 1rem;
  align-items: space-between;
  justify-content: center;
}

.question-input {
  flex: 1;
  padding: 1rem 1.25rem;
  border: 2px solid #e5e7eb;
  border-radius: 12px;
  font-size: 0.95rem;
  font-family: inherit;
  resize: none;
  max-height: 120px;
  min-height: 48px;
  transition: all 0.2s;
  background: #f9fafb;
  line-height: 1.5;
}

.question-input:focus {
  outline: none;
  border-color: #14b8a6;
  background: white;
  box-shadow: 0 0 0 4px rgba(20, 184, 166, 0.1);
}

.question-input::placeholder {
  color: #9ca3af;
}

.ask-btn {
  background: linear-gradient(135deg, #14b8a6, #0d9488);
  color: white;
  border: none;
  padding: 1rem 2rem;
  border-radius: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  white-space: nowrap;
  min-width: 100px;
  height: 48px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.95rem;
  box-shadow: 0 2px 8px rgba(20, 184, 166, 0.2);
}

.ask-btn:hover:not(.disabled) {
  background: linear-gradient(135deg, #0d9488, #0f766e);
  transform: translateY(-1px);
  box-shadow: 0 4px 12px rgba(20, 184, 166, 0.35);
}

.ask-btn:active:not(.disabled) {
  transform: translateY(0);
}

.ask-btn.disabled {
  opacity: 0.5;
  cursor: not-allowed;
  background: linear-gradient(135deg, #9ca3af, #6b7280);
  box-shadow: none;
}

.btn-spinner {
  width: 18px;
  height: 18px;
  border: 2px solid rgba(255, 255, 255, 0.3);
  border-top-color: white;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

.input-hint {
  margin-top: 0;
  padding-top: 0.5rem;
  font-size: 0.8rem;
  color: #9ca3af;
  text-align: center;
  border-top: 1px solid #f3f4f6;
}

.input-hint kbd {
  background: #f3f4f6;
  padding: 0.2rem 0.5rem;
  border-radius: 4px;
  font-family: "Monaco", "Menlo", "Ubuntu Mono", monospace;
  border: 1px solid #d1d5db;
  font-size: 0.75rem;
  color: #4b5563;
  font-weight: 500;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
  margin: 0 0.25rem;
}

/* Responsive Design - UPDATED */
@media (max-width: 768px) {
  .chat-modal {
    height: 95vh;
    max-height: none;
    border-radius: 16px 16px 0 0;
    margin-top: auto;
  }

  .modal-header {
    padding: 1rem 1.25rem;
  }

  .ai-avatar {
    width: 40px;
    height: 40px;
    font-size: 1.25rem;
  }

  .modal-title {
    font-size: 1.1rem;
  }

  .modal-subtitle {
    font-size: 0.8rem;
  }

  .modal-body {
    padding: 1rem;
  }

  .message-bubble {
    max-width: 90%;
    padding: 0.875rem 1rem;
  }

  .modal-footer {
    padding: 1rem 1rem 1.25rem 1rem;
  }

  .input-container {
    gap: 0.75rem;
    margin-bottom: 0.75rem;
  }

  .question-input {
    padding: 0.875rem 1rem;
    font-size: 0.9rem;
    min-height: 44px;
  }

  .ask-btn {
    padding: 0.875rem 1.25rem;
    min-width: 80px;
    height: 44px;
    font-size: 0.9rem;
  }

  .input-hint {
    font-size: 0.75rem;
    padding-top: 0.625rem;
  }

  .input-hint kbd {
    padding: 0.15rem 0.4rem;
    font-size: 0.7rem;
    margin: 0 0.15rem;
  }
}

@media (max-width: 480px) {
  .modal-footer {
    padding: 0.875rem 0.875rem 1rem 0.875rem;
  }

  .input-container {
    gap: 0.5rem;
  }

  .question-input {
    padding: 0.75rem 0.875rem;
    font-size: 0.875rem;
  }

  .ask-btn {
    padding: 0.75rem 1rem;
    min-width: 70px;
    font-size: 0.875rem;
  }

  .input-hint {
    font-size: 0.7rem;
  }
}
/* Responsive Design */
@media (max-width: 768px) {
  .chat-modal {
    height: 95vh;
    max-height: none;
    border-radius: 16px 16px 0 0;
    margin-top: auto;
  }

  .modal-header {
    padding: 1rem;
  }

  .ai-avatar {
    width: 40px;
    height: 40px;
    font-size: 1.25rem;
  }

  .modal-title {
    font-size: 1.1rem;
  }

  .modal-subtitle {
    font-size: 0.8rem;
  }

  .modal-body {
    padding: 1rem;
  }

  .message-bubble {
    max-width: 90%;
    padding: 0.875rem 1rem;
  }

  .input-container {
    gap: 0.5rem;
  }

  .question-input {
    padding: 0.75rem;
    font-size: 0.9rem;
  }

  .ask-btn {
    padding: 0.75rem 1rem;
    min-width: 70px;
    font-size: 0.875rem;
  }
}
</style>
