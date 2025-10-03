<template>
  <div class="page-header">
    <nav class="breadcrumb">
      <router-link to="/dashboard" class="breadcrumb-item"
        >Dashboard</router-link
      >
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item">Merchants</span>
      <span class="breadcrumb-separator">/</span>
      <router-link
        :to="`/merchant/${merchantId}`"
        class="breadcrumb-item"
        v-if="merchantId"
      >
        {{ merchantName || "Loading..." }}
      </router-link>
      <span class="breadcrumb-item" v-else>{{
        merchantName || "Loading..."
      }}</span>
      <span class="breadcrumb-separator">/</span>
      <span class="breadcrumb-item current">Social Updates</span>
    </nav>
  </div>

  <!-- Themed Loading State -->
  <div v-if="isLoading" class="loading-container">
    <div class="loader-wrapper">
      <div class="themed-loader">
        <!-- Main Spinner -->
        <div class="loader-spinner">
          <div class="spinner-ring"></div>
          <div class="spinner-inner">
            <div class="spinner-icon">📊</div>
          </div>
        </div>

        <!-- Animated Dots -->
        <div class="loader-dots">
          <div class="dot"></div>
          <div class="dot"></div>
          <div class="dot"></div>
        </div>

        <!-- Progress Bar -->
        <div class="progress-container">
          <div class="progress-bar">
            <div class="progress-fill"></div>
          </div>
        </div>
      </div>

      <div class="loading-text">
        <h3>Loading Social Data</h3>
        <p>
          Fetching latest updates for
          <span class="merchant-highlight">{{ merchantName }}</span
          >...
        </p>
        <div class="loading-steps">
          <div class="step">📰 News articles</div>
          <div class="step">🔗 Reddit discussions</div>
          <div class="step">⭐ Customer reviews</div>
          <div class="step">🐦 Social media posts</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Main Dashboard Content -->
  <div v-else class="data-dashboard">
    <!-- Header Buttons -->
    <div class="header-buttons">
      <button class="back-btn" @click="goBack">← Back</button>
      <!-- <button class="ai-summary-btn" @click="generateAISummary">
        🤖 AI Summary
      </button> -->
      <VertexAI :socialData="socialData" :merchantName="merchantName" />
    </div>

    <!-- News Section -->
    <section class="data-section">
      <h2 class="section-title">📰 Latest News</h2>
      <div class="carousel-container">
        <div class="carousel" ref="newsCarousel">
          <div
            v-for="article in socialData.news"
            :key="article.article_id"
            class="card news-card"
          >
            <div class="card-header">
              <h3 class="card-title">{{ article.title }}</h3>
              <span class="sentiment-badge" :class="article.sentiment_label">
                {{ article.sentiment_label }}
              </span>
            </div>
            <p class="card-subtitle" v-if="article.subtitle">
              {{ article.subtitle }}
            </p>
            <div class="card-content">
              <p class="content-preview">
                {{ truncateText(article.content, 120) }}
              </p>
              <div class="card-meta">
                <span class="publisher">{{ article.publisher }}</span>
                <span class="date">{{ formatDate(article.published_at) }}</span>
              </div>
              <div class="card-stats">
                <span class="stat">👁 {{ article.pageviews }}</span>
                <span class="stat">🔥 {{ Math.round(article.hot_score) }}</span>
                <span class="stat"
                  >📊 {{ article.risk_score.toFixed(1) }}%</span
                >
              </div>
              <button class="details-btn" @click="openModal('news', article)">
                Details
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- Reddit Section -->
    <section class="data-section">
      <h2 class="section-title">🔗 Reddit Discussions</h2>
      <div class="carousel-container">
        <div class="carousel" ref="redditCarousel">
          <div
            v-for="post in socialData.reddit"
            :key="post.id"
            class="card reddit-card"
          >
            <div class="card-header">
              <h3 class="card-title">{{ post.title }}</h3>
              <span class="sentiment-badge" :class="post.sentiment_label">
                {{ post.sentiment_label }}
              </span>
            </div>
            <div class="card-content">
              <p class="content-preview" v-if="post.selftext">
                {{ truncateText(post.selftext, 80) }}
              </p>
              <div class="card-meta">
                <span class="subreddit">{{ post.subreddit }}</span>
                <span class="author">by {{ post.author }}</span>
              </div>
              <div class="card-stats">
                <span class="stat">⬆ {{ post.score }}</span>
                <span class="stat">💬 {{ post.num_comments }}</span>
                <span class="stat"
                  >📈 {{ (post.upvote_ratio * 100).toFixed(0) }}%</span
                >
              </div>
              <button class="details-btn" @click="openModal('reddit', post)">
                Details
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- Reviews Section -->
    <section class="data-section">
      <h2 class="section-title">⭐ Customer Reviews</h2>
      <div class="carousel-container">
        <div class="carousel" ref="reviewsCarousel">
          <div
            v-for="review in socialData.reviews"
            :key="review.review_id"
            class="card review-card"
          >
            <div class="card-header">
              <h3 class="card-title">{{ review.title }}</h3>
              <div class="rating">
                <span
                  v-for="i in 5"
                  :key="i"
                  class="star"
                  :class="{ filled: i <= review.rating }"
                  >★</span
                >
              </div>
            </div>
            <div class="card-content">
              <p class="content-preview">
                {{ truncateText(review.body, 100) }}
              </p>
              <div class="card-meta">
                <span class="product">{{ review.product_name }}</span>
                <span class="username">{{ review.username }}</span>
              </div>
              <div class="card-stats">
                <span class="stat">👀 {{ review.views }}</span>
                <span class="stat" v-if="review.verified_purchase"
                  >✓ Verified</span
                >
                <span class="stat">{{ review.country }}</span>
              </div>
              <button class="details-btn" @click="openModal('review', review)">
                Details
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- Tweets Section -->
    <section class="data-section">
      <h2 class="section-title">🐦 Social Media</h2>
      <div class="carousel-container">
        <div class="carousel" ref="tweetsCarousel">
          <div
            v-for="tweet in socialData.tweet"
            :key="tweet.tweet_id"
            class="card tweet-card"
          >
            <div class="card-content">
              <p class="tweet-content">{{ tweet.content }}</p>
              <div class="card-meta">
                <span class="date">{{ formatDate(tweet.created_at) }}</span>
                <span v-if="tweet.entities.hashtags.length" class="hashtags">
                  {{ tweet.entities.hashtags.map((h) => h).join(" ") }}
                </span>
              </div>
              <div class="card-stats">
                <span class="stat"
                  >❤ {{ tweet.public_metrics.like_count }}</span
                >
                <span class="stat"
                  >🔄 {{ tweet.public_metrics.retweet_count }}</span
                >
                <span class="stat"
                  >💬 {{ tweet.public_metrics.reply_count }}</span
                >
              </div>
              <button class="details-btn" @click="openModal('tweet', tweet)">
                Details
              </button>
            </div>
          </div>
        </div>
      </div>
    </section>

    <!-- Modal -->
    <div v-if="showModal" class="modal-overlay" @click="closeModal">
      <div class="modal-content" @click.stop>
        <div class="modal-header">
          <h2 class="modal-title">{{ getModalTitle() }}</h2>
          <button class="close-btn" @click="closeModal">×</button>
        </div>

        <div class="modal-body">
          <!-- News Modal -->
          <div v-if="modalType === 'news'" class="news-details">
            <div class="detail-row">
              <strong>Title:</strong> {{ selectedItem.title }}
            </div>
            <div class="detail-row" v-if="selectedItem.subtitle">
              <strong>Subtitle:</strong> {{ selectedItem.subtitle }}
            </div>
            <div class="detail-row">
              <strong>Content:</strong>
              <p class="full-content">{{ selectedItem.content }}</p>
            </div>
            <div class="detail-row">
              <strong>Publisher:</strong> {{ selectedItem.publisher }}
            </div>
            <div class="detail-row">
              <strong>Author:</strong> {{ selectedItem.author }}
            </div>
            <div class="detail-row">
              <strong>Published:</strong>
              {{ formatDate(selectedItem.published_at) }}
            </div>
            <div class="detail-row">
              <strong>Section:</strong> {{ selectedItem.section }}
            </div>
            <div class="detail-row">
              <strong>Categories:</strong>
              {{ selectedItem.categories.join(", ") }}
            </div>
            <div class="detail-row">
              <strong>Keywords:</strong> {{ selectedItem.keywords.join(", ") }}
            </div>
            <div class="detail-row">
              <strong>Sentiment:</strong>
              <span
                class="sentiment-badge"
                :class="selectedItem.sentiment_label"
              >
                {{ selectedItem.sentiment_label }} ({{
                  selectedItem.sentiment_score.toFixed(3)
                }})
              </span>
            </div>
            <div class="detail-row">
              <strong>Metrics:</strong>
              <div class="metrics-grid">
                <span class="metric"
                  >👁 Views: {{ selectedItem.pageviews }}</span
                >
                <span class="metric"
                  >🔥 Hot Score: {{ Math.round(selectedItem.hot_score) }}</span
                >
                <span class="metric"
                  >📊 Risk Score:
                  {{ selectedItem.risk_score.toFixed(1) }}%</span
                >
                <span class="metric">📤 Shares: {{ selectedItem.shares }}</span>
                <span class="metric"
                  >💬 Comments: {{ selectedItem.comments }}</span
                >
                <span class="metric"
                  >⏱ Reading Time: {{ selectedItem.reading_time_min }} min</span
                >
              </div>
            </div>
            <div class="detail-row" v-if="selectedItem.url">
              <strong>URL:</strong>
              <a
                :href="selectedItem.url"
                target="_blank"
                class="external-link"
                >{{ selectedItem.url }}</a
              >
            </div>
          </div>

          <!-- Reddit Modal -->
          <div v-if="modalType === 'reddit'" class="reddit-details">
            <div class="detail-row">
              <strong>Title:</strong> {{ selectedItem.title }}
            </div>
            <div class="detail-row" v-if="selectedItem.selftext">
              <strong>Content:</strong>
              <p class="full-content">{{ selectedItem.selftext }}</p>
            </div>
            <div class="detail-row">
              <strong>Subreddit:</strong> {{ selectedItem.subreddit }}
            </div>
            <div class="detail-row">
              <strong>Author:</strong> {{ selectedItem.author }}
            </div>
            <div class="detail-row">
              <strong>Created:</strong>
              {{ formatDate(selectedItem.created_at) }}
            </div>
            <div class="detail-row">
              <strong>Flair:</strong> {{ selectedItem.flair_text || "None" }}
            </div>
            <div class="detail-row">
              <strong>Keywords:</strong> {{ selectedItem.keywords.join(", ") }}
            </div>
            <div class="detail-row">
              <strong>Sentiment:</strong>
              <span
                class="sentiment-badge"
                :class="selectedItem.sentiment_label"
              >
                {{ selectedItem.sentiment_label }} ({{
                  selectedItem.sentiment_score.toFixed(3)
                }})
              </span>
            </div>
            <div class="detail-row">
              <strong>Metrics:</strong>
              <div class="metrics-grid">
                <span class="metric">⬆ Score: {{ selectedItem.score }}</span>
                <span class="metric"
                  >💬 Comments: {{ selectedItem.num_comments }}</span
                >
                <span class="metric"
                  >📈 Upvote Ratio:
                  {{ (selectedItem.upvote_ratio * 100).toFixed(1) }}%</span
                >
                <span class="metric"
                  >📊 Risk Score:
                  {{ selectedItem.risk_score.toFixed(1) }}%</span
                >
              </div>
            </div>
            <div class="detail-row" v-if="selectedItem.url">
              <strong>URL:</strong>
              <a
                :href="selectedItem.url"
                target="_blank"
                class="external-link"
                >{{ selectedItem.url }}</a
              >
            </div>
          </div>

          <!-- Review Modal -->
          <div v-if="modalType === 'review'" class="review-details">
            <div class="detail-row">
              <strong>Title:</strong> {{ selectedItem.title }}
            </div>
            <div class="detail-row">
              <strong>Rating:</strong>
              <div class="rating">
                <span
                  v-for="i in 5"
                  :key="i"
                  class="star"
                  :class="{ filled: i <= selectedItem.rating }"
                  >★</span
                >
                ({{ selectedItem.rating }}/5)
              </div>
            </div>
            <div class="detail-row">
              <strong>Review:</strong>
              <p class="full-content">{{ selectedItem.body }}</p>
            </div>
            <div class="detail-row">
              <strong>Product:</strong> {{ selectedItem.product_name }}
            </div>
            <div class="detail-row">
              <strong>SKU:</strong> {{ selectedItem.sku }}
            </div>
            <div class="detail-row">
              <strong>Username:</strong> {{ selectedItem.username }}
            </div>
            <div class="detail-row">
              <strong>Created:</strong>
              {{ formatDate(selectedItem.created_at) }}
            </div>
            <div class="detail-row">
              <strong>Country:</strong> {{ selectedItem.country }}
            </div>
            <div class="detail-row">
              <strong>Platform:</strong> {{ selectedItem.platform }}
            </div>
            <div class="detail-row">
              <strong>Verified Purchase:</strong>
              {{ selectedItem.verified_purchase ? "Yes" : "No" }}
            </div>
            <div class="detail-row">
              <strong>Tags:</strong> {{ selectedItem.tags.join(", ") }}
            </div>
            <div class="detail-row">
              <strong>Sentiment:</strong>
              <span
                class="sentiment-badge"
                :class="selectedItem.sentiment_label"
              >
                {{ selectedItem.sentiment_label }} ({{
                  selectedItem.sentiment_score.toFixed(3)
                }})
              </span>
            </div>
            <div class="detail-row">
              <strong>Metrics:</strong>
              <div class="metrics-grid">
                <span class="metric">👀 Views: {{ selectedItem.views }}</span>
                <span class="metric"
                  >👍 Helpful: {{ selectedItem.helpful_votes }}</span
                >
                <span class="metric"
                  >👎 Unhelpful: {{ selectedItem.unhelpful_votes }}</span
                >
              </div>
            </div>
          </div>

          <!-- Tweet Modal -->
          <div v-if="modalType === 'tweet'" class="tweet-details">
            <div class="detail-row">
              <strong>Content:</strong>
              <p class="full-content">{{ selectedItem.content }}</p>
            </div>
            <div class="detail-row">
              <strong>Created:</strong>
              {{ formatDate(selectedItem.created_at) }}
            </div>
            <div class="detail-row">
              <strong>Language:</strong> {{ selectedItem.lang }}
            </div>
            <div
              class="detail-row"
              v-if="selectedItem.entities.hashtags.length"
            >
              <strong>Hashtags:</strong>
              {{ selectedItem.entities.hashtags.join(", ") }}
            </div>
            <div
              class="detail-row"
              v-if="selectedItem.entities.mentions.length"
            >
              <strong>Mentions:</strong>
              {{ selectedItem.entities.mentions.join(", ") }}
            </div>
            <div class="detail-row">
              <strong>Public Metrics:</strong>
              <div class="metrics-grid">
                <span class="metric"
                  >❤ Likes: {{ selectedItem.public_metrics.like_count }}</span
                >
                <span class="metric"
                  >🔄 Retweets:
                  {{ selectedItem.public_metrics.retweet_count }}</span
                >
                <span class="metric"
                  >💬 Replies:
                  {{ selectedItem.public_metrics.reply_count }}</span
                >
                <span class="metric"
                  >📝 Quotes:
                  {{ selectedItem.public_metrics.quote_count }}</span
                >
              </div>
            </div>
            <div class="detail-row">
              <strong>Engagement Metrics:</strong>
              <div class="metrics-grid">
                <span class="metric"
                  >👁 Impressions:
                  {{ selectedItem.non_public_metrics.impression_count }}</span
                >
                <span class="metric"
                  >🔗 Link Clicks:
                  {{ selectedItem.non_public_metrics.url_link_clicks }}</span
                >
                <span class="metric"
                  >👤 Profile Clicks:
                  {{
                    selectedItem.non_public_metrics.user_profile_clicks
                  }}</span
                >
              </div>
            </div>
            <div class="detail-row">
              <strong>Sensitive Content:</strong>
              {{ selectedItem.possibly_sensitive ? "Yes" : "No" }}
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- AI Summary Modal -->
    <div
      v-if="showAISummaryModal"
      class="modal-overlay"
      @click="closeAISummaryModal"
    >
      <div class="modal-content ai-summary-modal" @click.stop>
        <div class="modal-header">
          <h2 class="modal-title">🤖 AI Summary</h2>
          <button class="close-btn" @click="closeAISummaryModal">×</button>
        </div>
        <div class="modal-body">
          <div class="ai-summary-content">
            <p>{{ aiSummary }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
// Import axios for making HTTP requests
import axios from "axios";
import VertexAI from "./VertexAI.vue";

export default {
  name: "SocialDashboard",
  components: {
    VertexAI,
  },
  data() {
    return {
      socialData: {
        news: [],
        reddit: [],
        reviews: [],
        tweet: [],
      },
      showModal: false,
      showAISummaryModal: false,
      modalType: "",
      selectedItem: null,
      aiSummary: "",
      merchantName: "",
      merchantId: "",
      backendEndpoint: "", // Backend endpoint will be constructed
      isLoading: false, // Loading state
    };
  },
  async created() {
    // Extract merchantName from the URL path directly
    // For URL like: http://localhost:3000/explore/BuildPro
    const currentPath = this.$route.path; // Gets: /explore/BuildPro
    const pathSegments = currentPath
      .split("/")
      .filter((segment) => segment !== "");

    // Find the segment after 'explore'
    const exploreIndex = pathSegments.findIndex(
      (segment) => segment === "explore"
    );
    if (exploreIndex !== -1 && exploreIndex + 1 < pathSegments.length) {
      this.merchantName = pathSegments[exploreIndex + 1];
    } else {
      // Fallback: try route params
      this.merchantName =
        this.$route.params.merchantName ||
        this.$route.params.merchantname ||
        this.$route.params.id ||
        "Unknown Merchant";
    }

    // Also set merchantId for backward compatibility
    this.merchantId = this.merchantName;

    const simNow = this.$route.query.simNow || new Date().toISOString();

    // Construct the backend endpoint using the merchant name
    this.backendEndpoint = `${import.meta.env.VITE_API_BASE || 'http://localhost:8000'}/v1/${this.merchantName}/data?streams=all&order=desc&limit=5000&window=90d&allow_future=false&now=${simNow}&include_stock_meta=true`;

    console.log("Current path:", currentPath);
    console.log("Path segments:", pathSegments);
    console.log("Extracted merchant name:", this.merchantName);
    console.log("SimNow from query:", simNow);
    console.log("Constructed endpoint:", this.backendEndpoint);

    // Fetch data from the backend endpoint
    await this.fetchSocialData();
  },
  methods: {
    async fetchSocialData() {
      this.isLoading = true; // Start loading

      try {
        console.log("Fetching data from:", this.backendEndpoint);
        console.log("Merchant name extracted from URL:", this.merchantName);

        const response = await axios.get(this.backendEndpoint);

        // Determine the payload shape robustly
        const dataRoot =
          response && response.data && response.data.data
            ? response.data.data
            : response?.data ?? {};

        // Normalize potential alternative key names
        const rawNews = Array.isArray(dataRoot.news) ? dataRoot.news : [];
        const rawReddit = Array.isArray(dataRoot.reddit) ? dataRoot.reddit : [];
        const rawReviews = Array.isArray(dataRoot.reviews)
          ? dataRoot.reviews
          : [];
        const rawTweets = Array.isArray(dataRoot.tweet)
          ? dataRoot.tweet
          : Array.isArray(dataRoot.tweets)
          ? dataRoot.tweets
          : [];

        // Helper to sort by date key (newest first)
        const sortDescByDate = (arr, dateKey) =>
          arr
            .slice()
            .sort((a, b) => new Date(b[dateKey]) - new Date(a[dateKey]));

        // Filter to latest 20 items
        const filteredNews = sortDescByDate(rawNews, "published_at").slice(
          0,
          20
        );
        const filteredReddit = sortDescByDate(rawReddit, "created_at").slice(
          0,
          20
        );
        const filteredReviews = sortDescByDate(rawReviews, "created_at").slice(
          0,
          20
        );
        const filteredTweets = sortDescByDate(rawTweets, "created_at").slice(
          0,
          20
        );

        // Store filtered data into their respective arrays
        this.socialData = {
          news: filteredNews,
          reddit: filteredReddit,
          reviews: filteredReviews,
          tweet: filteredTweets,
        };

        console.log("Consolidated Social Data:", this.socialData);

        // Console log the filtered data
        console.log("Fetched News (latest 20):", filteredNews);
        console.log("Fetched Reddit (latest 20):", filteredReddit);
        console.log("Fetched Reviews (latest 20):", filteredReviews);
        console.log("Fetched Tweets (latest 20):", filteredTweets);
      } catch (error) {
        console.error("Error fetching social data:", error);
        console.error("Failed endpoint:", this.backendEndpoint);
        console.error("Merchant name:", this.merchantName);
        // Set default empty state for socialData
        this.socialData = {
          news: [],
          reddit: [],
          reviews: [],
          tweet: [],
        };
      } finally {
        this.isLoading = false; // Stop loading
      }
    },
    goBack() {
      // Navigate back to explore page
      this.$router.go(-1);
    },
    truncateText(text, maxLength) {
      if (!text) return "";
      return text.length > maxLength
        ? text.substring(0, maxLength) + "..."
        : text;
    },
    formatDate(dateString) {
      const date = new Date(dateString);
      return date.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      });
    },
    openModal(type, item) {
      this.modalType = type;
      this.selectedItem = item;
      this.showModal = true;
      document.body.style.overflow = "hidden";
    },
    closeModal() {
      this.showModal = false;
      this.modalType = "";
      this.selectedItem = null;
      document.body.style.overflow = "auto";
    },
    getModalTitle() {
      switch (this.modalType) {
        case "news":
          return "📰 News Article Details";
        case "reddit":
          return "🔗 Reddit Post Details";
        case "review":
          return "⭐ Review Details";
        case "tweet":
          return "🐦 Tweet Details";
        default:
          return "Details";
      }
    },
    generateAISummary() {
      // Simulate AI summary generation
      this.aiSummary = "Loading AI summary...";
      this.showAISummaryModal = true;

      // Simulate API call delay
      setTimeout(() => {
        const totalNews = this.socialData.news.length;
        const totalReddit = this.socialData.reddit.length;
        const totalReviews = this.socialData.reviews.length;
        const totalTweets = this.socialData.tweet.length;

        // Calculate sentiment distribution
        const allItems = [
          ...this.socialData.news,
          ...this.socialData.reddit,
          ...this.socialData.reviews,
          ...this.socialData.tweet.map((t) => ({ sentiment_label: "neutral" })),
        ];

        const positive = allItems.filter(
          (item) => item.sentiment_label === "positive"
        ).length;
        const negative = allItems.filter(
          (item) => item.sentiment_label === "negative"
        ).length;
        const neutral = allItems.filter(
          (item) => item.sentiment_label === "neutral"
        ).length;

        this.aiSummary = `📊 **${
          this.merchantName
        } Social Media Analysis Summary**
        
**Overall Activity:** 
- ${totalNews} news articles
- ${totalReddit} Reddit discussions  
- ${totalReviews} customer reviews
- ${totalTweets} social media mentions

**Sentiment Analysis:**
- Positive: ${positive} items (${((positive / allItems.length) * 100).toFixed(
          1
        )}%)
- Negative: ${negative} items (${((negative / allItems.length) * 100).toFixed(
          1
        )}%)
- Neutral: ${neutral} items (${((neutral / allItems.length) * 100).toFixed(1)}%)

**Key Insights:**
- Recent news coverage shows mixed reactions to ${
          this.merchantName
        }'s latest moves
- Reddit discussions indicate community interest with varied opinions
- Customer reviews are predominantly positive with high ratings
- Social media engagement appears moderate with growing attention

**Risk Assessment:** Moderate risk identified from regulatory discussions and mixed sentiment patterns. Monitor closely for developing trends.

**Recommendation:** Continue monitoring social sentiment and engage proactively with community feedback to maintain positive brand perception.`;
      }, 2000);
    },
    closeAISummaryModal() {
      this.showAISummaryModal = false;
      document.body.style.overflow = "auto";
    },
  },
  beforeUnmount() {
    document.body.style.overflow = "auto";
  },
};
</script>

<style scoped>
/* Themed Loading Styles - Teal, White, Grey Theme */
.loading-container {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 50%, #f1f5f9 100%);
  padding: 2rem;
}

.loader-wrapper {
  text-align: center;
  background: #ffffff;
  padding: 3rem 2.5rem;
  border-radius: 20px;
  box-shadow: 0 20px 40px rgba(20, 184, 166, 0.1), 0 1px 3px rgba(0, 0, 0, 0.1);
  border: 1px solid #e5e7eb;
  max-width: 450px;
  width: 100%;
  position: relative;
  overflow: hidden;
}

.loader-wrapper::before {
  content: "";
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  height: 4px;
  background: linear-gradient(90deg, #14b8a6, #0d9488, #14b8a6);
  background-size: 200% 100%;
  animation: shimmer 2s infinite;
}

.themed-loader {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2rem;
  margin-bottom: 2rem;
}

.loader-spinner {
  position: relative;
  width: 80px;
  height: 80px;
}

.spinner-ring {
  position: absolute;
  width: 100%;
  height: 100%;
  border: 3px solid #e5e7eb;
  border-top: 3px solid #14b8a6;
  border-right: 3px solid #0d9488;
  border-radius: 50%;
  animation: spin 1.2s linear infinite;
}

.spinner-inner {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 50px;
  height: 50px;
  background: linear-gradient(135deg, #14b8a6, #0d9488);
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  animation: counterSpin 1.2s linear infinite;
}

.spinner-icon {
  font-size: 1.5rem;
  animation: bounce 1.5s ease-in-out infinite;
}

.loader-dots {
  display: flex;
  gap: 8px;
  align-items: center;
}

.dot {
  width: 12px;
  height: 12px;
  background: linear-gradient(135deg, #14b8a6, #0d9488);
  border-radius: 50%;
  animation: dotPulse 1.8s ease-in-out infinite;
}

.dot:nth-child(2) {
  animation-delay: 0.3s;
}

.dot:nth-child(3) {
  animation-delay: 0.6s;
}

.progress-container {
  width: 100%;
  max-width: 300px;
}

.progress-bar {
  width: 100%;
  height: 6px;
  background: #e5e7eb;
  border-radius: 3px;
  overflow: hidden;
}

.progress-fill {
  height: 100%;
  background: linear-gradient(90deg, #14b8a6, #0d9488, #14b8a6);
  background-size: 200% 100%;
  border-radius: 3px;
  animation: progressFill 2s ease-in-out infinite;
}

.loading-text {
  color: #374151;
}

.loading-text h3 {
  font-size: 1.5rem;
  font-weight: 700;
  margin: 0 0 0.5rem 0;
  color: #1f2937;
}

.loading-text p {
  font-size: 1rem;
  margin: 0 0 1.5rem 0;
  color: #6b7280;
  line-height: 1.5;
}

.merchant-highlight {
  color: #14b8a6;
  font-weight: 600;
}

.loading-steps {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 0.75rem;
  margin-top: 1rem;
}

.step {
  background: linear-gradient(135deg, #f0fdfa, #ecfdf5);
  border: 1px solid #a7f3d0;
  padding: 0.5rem 0.75rem;
  border-radius: 8px;
  font-size: 0.875rem;
  color: #065f46;
  font-weight: 500;
  animation: stepGlow 2s ease-in-out infinite;
}

.step:nth-child(1) {
  animation-delay: 0s;
}
.step:nth-child(2) {
  animation-delay: 0.5s;
}
.step:nth-child(3) {
  animation-delay: 1s;
}
.step:nth-child(4) {
  animation-delay: 1.5s;
}

/* Animations */
@keyframes spin {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}

@keyframes counterSpin {
  0% {
    transform: translate(-50%, -50%) rotate(0deg);
  }
  100% {
    transform: translate(-50%, -50%) rotate(-360deg);
  }
}

@keyframes bounce {
  0%,
  100% {
    transform: translateY(0);
  }
  50% {
    transform: translateY(-5px);
  }
}

@keyframes dotPulse {
  0%,
  100% {
    transform: scale(0.8);
    opacity: 0.6;
  }
  50% {
    transform: scale(1.2);
    opacity: 1;
  }
}

@keyframes progressFill {
  0% {
    transform: translateX(-100%);
  }
  100% {
    transform: translateX(100%);
  }
}

@keyframes shimmer {
  0% {
    background-position: -200% 0;
  }
  100% {
    background-position: 200% 0;
  }
}

@keyframes stepGlow {
  0%,
  100% {
    box-shadow: 0 0 0 rgba(20, 184, 166, 0.3);
    border-color: #a7f3d0;
  }
  50% {
    box-shadow: 0 0 20px rgba(20, 184, 166, 0.4);
    border-color: #14b8a6;
  }
}

/* Existing Dashboard Styles */
.data-dashboard {
  padding: 2rem;
  background-color: #f8fafc;
  min-height: 100vh;
}

.header-buttons {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 2rem;
}

.back-btn {
  background: linear-gradient(135deg, #6b7280, #4b5563);
  color: white;
  border: none;
  padding: 0.75rem 1.5rem;
  border-radius: 10px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  box-shadow: 0 2px 4px rgba(107, 114, 128, 0.2);
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.back-btn:hover {
  background: linear-gradient(135deg, #4b5563, #374151);
  transform: translateY(-1px);
  box-shadow: 0 4px 8px rgba(107, 114, 128, 0.3);
}

.ai-summary-btn {
  background: linear-gradient(135deg, #14b8a6, #0d9488);
  color: white;
  border: none;
  padding: 0.75rem 1.5rem;
  border-radius: 10px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  box-shadow: 0 2px 4px rgba(20, 184, 166, 0.2);
}

.ai-summary-btn:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 8px rgba(20, 184, 166, 0.3);
}

.data-section {
  margin-bottom: 3rem;
}

.section-title {
  font-size: 1.5rem;
  font-weight: 600;
  color: #374151;
  margin-bottom: 1rem;
  padding-left: 0.5rem;
}

.carousel-container {
  position: relative;
  overflow: hidden;
}

.carousel {
  display: flex;
  gap: 1rem;
  overflow-x: auto;
  padding: 0.5rem 0;
  scroll-behavior: smooth;
  scrollbar-width: thin;
  scrollbar-color: #14b8a6 #e5e7eb;
}

.carousel::-webkit-scrollbar {
  height: 6px;
}

.carousel::-webkit-scrollbar-track {
  background: #e5e7eb;
  border-radius: 3px;
}

.carousel::-webkit-scrollbar-thumb {
  background: #14b8a6;
  border-radius: 3px;
}

.card {
  flex: 0 0 320px;
  background: white;
  border-radius: 12px;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  border: 1px solid #e5e7eb;
  transition: transform 0.2s, box-shadow 0.2s;
  display: flex;
  flex-direction: column;
}

.card:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(20, 184, 166, 0.15);
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 0.75rem;
  gap: 0.75rem;
}

.card-title {
  font-size: 1rem;
  font-weight: 600;
  color: #1f2937;
  line-height: 1.4;
  flex: 1;
  margin: 0;
}

.card-subtitle {
  color: #6b7280;
  font-size: 0.875rem;
  margin-bottom: 0.75rem;
  line-height: 1.4;
}

.card-content {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.content-preview {
  color: #4b5563;
  font-size: 0.875rem;
  line-height: 1.5;
  margin-bottom: 1rem;
  flex: 1;
}

.tweet-content {
  color: #1f2937;
  font-size: 0.95rem;
  line-height: 1.5;
  margin-bottom: 1rem;
  flex: 1;
}

.card-meta {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.75rem;
  font-size: 0.75rem;
  color: #6b7280;
}

.card-stats {
  display: flex;
  gap: 0.75rem;
  flex-wrap: wrap;
  margin-bottom: 1rem;
}

.stat {
  font-size: 0.75rem;
  color: #6b7280;
  background: #f3f4f6;
  padding: 0.25rem 0.5rem;
  border-radius: 6px;
}

.details-btn {
  background: #14b8a6;
  color: white;
  border: none;
  padding: 0.5rem 1rem;
  border-radius: 6px;
  font-weight: 500;
  cursor: pointer;
  transition: background-color 0.2s;
  margin-top: auto;
}

.details-btn:hover {
  background: #0d9488;
}

.sentiment-badge {
  font-size: 0.75rem;
  padding: 0.25rem 0.5rem;
  border-radius: 6px;
  font-weight: 500;
  text-transform: uppercase;
  letter-spacing: 0.025em;
}

.sentiment-badge.positive {
  background: #d1fae5;
  color: #065f46;
}

.sentiment-badge.negative {
  background: #fee2e2;
  color: #991b1b;
}

.sentiment-badge.neutral {
  background: #e5e7eb;
  color: #374151;
}

.rating {
  display: flex;
  gap: 1px;
  align-items: center;
}

.star {
  color: #d1d5db;
  font-size: 1rem;
}

.star.filled {
  color: #fbbf24;
}

.publisher,
.subreddit,
.product {
  font-weight: 500;
  color: #14b8a6;
}

.hashtags {
  color: #14b8a6;
  font-weight: 500;
}

/* Modal Styles */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  padding: 1rem;
}

.modal-content {
  background: white;
  border-radius: 12px;
  max-width: 800px;
  width: 100%;
  max-height: 80vh;
  overflow: hidden;
  box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1);
}

.modal-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1.5rem;
  border-bottom: 1px solid #e5e7eb;
  background: #f9fafb;
}

.modal-title {
  font-size: 1.25rem;
  font-weight: 600;
  color: #1f2937;
  margin: 0;
}

.close-btn {
  background: none;
  border: none;
  font-size: 1.5rem;
  cursor: pointer;
  color: #6b7280;
  width: 32px;
  height: 32px;
  border-radius: 6px;
  display: flex;
  align-items: center;
  justify-content: center;
}

.close-btn:hover {
  background: #e5e7eb;
  color: #374151;
}

.modal-body {
  padding: 1.5rem;
  overflow-y: auto;
  max-height: calc(80vh - 80px);
}

.detail-row {
  margin-bottom: 1rem;
  padding-bottom: 0.75rem;
  border-bottom: 1px solid #f3f4f6;
}

.detail-row:last-child {
  border-bottom: none;
  margin-bottom: 0;
}

.detail-row strong {
  color: #374151;
  display: block;
  margin-bottom: 0.25rem;
}

.full-content {
  color: #4b5563;
  line-height: 1.6;
  margin: 0.5rem 0 0 0;
  padding: 0.75rem;
  background: #f9fafb;
  border-radius: 6px;
  border-left: 3px solid #14b8a6;
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 0.5rem;
  margin-top: 0.5rem;
}

.metric {
  background: #f3f4f6;
  padding: 0.5rem;
  border-radius: 6px;
  font-size: 0.875rem;
  color: #4b5563;
}

.external-link {
  color: #14b8a6;
  text-decoration: none;
  word-break: break-all;
}

.external-link:hover {
  text-decoration: underline;
}

.ai-summary-modal {
  max-width: 600px;
}

.ai-summary-content {
  white-space: pre-line;
  line-height: 1.6;
  color: #374151;
}

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

.breadcrumb-item {
  color: #6b7280;
  text-decoration: none;
  transition: color 0.2s ease;
}

.breadcrumb-item:hover {
  color: #14b8a6;
}

.breadcrumb-item.current {
  color: #14b8a6;
  font-weight: 500;
}

.breadcrumb-separator {
  color: #d1d5db;
  user-select: none;
}

/* Responsive adjustments */
@media (max-width: 768px) {
  .loading-container {
    padding: 1rem;
  }

  .loader-wrapper {
    padding: 2rem 1.5rem;
    border-radius: 16px;
  }

  .loading-steps {
    grid-template-columns: 1fr;
    gap: 0.5rem;
  }

  .data-dashboard {
    padding: 1rem;
  }

  .header-buttons {
    flex-direction: column;
    gap: 1rem;
    align-items: stretch;
  }

  .back-btn,
  .ai-summary-btn {
    padding: 0.65rem 1.25rem;
    justify-content: center;
  }

  .card {
    flex: 0 0 280px;
    padding: 1rem;
  }

  .section-title {
    font-size: 1.25rem;
  }

  .modal-content {
    margin: 1rem;
    max-height: 90vh;
  }

  .metrics-grid {
    grid-template-columns: 1fr;
  }
}
</style>
