// Locations Dropdown functionality
function toggleDropdown() {
    const dropdown = document.querySelector('.dropdown');
    dropdown.classList.toggle('open');
    
    // Close dropdown when clicking outside
    document.addEventListener('click', function closeOnClickOutside(e) {
        if (!dropdown.contains(e.target)) {
            dropdown.classList.remove('open');
            document.removeEventListener('click', closeOnClickOutside);
        }
    });
}

function closeDropdown() {
    const dropdown = document.querySelector('.dropdown');
    dropdown.classList.remove('open');
}

// Chat functionality
function startChat() {
    // Check if chat is available based on EST time
    const { day, hour } = getESTDateTime();
    
    let isOnline = false;
    let message = '';
    
    if (day >= 1 && day <= 5) { // Monday to Friday
        if (hour >= 9 && hour < 17) { // 9AM to 5PM EST
            isOnline = true;
            message = 'Connecting you to a live agent...';
        } else {
            message = 'Our agents are currently offline. You can leave a message and we\'ll get back to you within 24 hours.';
        }
    } else { // Weekend
        message = 'Our agents are currently offline during weekends. You can leave a message and we\'ll get back to you within 24 hours.';
    }
    
    // Show chat interface or redirect to chat platform
    if (isOnline) {
        // Replace with actual chat integration (e.g., Intercom, Zendesk, etc.)
        showChatModal(message, true);
    } else {
        showChatModal(message, false);
    }
}

function showChatModal(message, isOnline) {
    // Create modal overlay
    const overlay = document.createElement('div');
    overlay.style.cssText = `
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0,0,0,0.5);
        z-index: 10000;
        display: flex;
        align-items: center;
        justify-content: center;
    `;
    
    // Create modal content
    const modal = document.createElement('div');
    modal.style.cssText = `
        background: white;
        padding: 2rem;
        border-radius: 12px;
        max-width: 500px;
        width: 90%;
        text-align: center;
        box-shadow: 0 20px 40px rgba(0,0,0,0.2);
    `;
    
    modal.innerHTML = `
        <div style="font-size: 3rem; margin-bottom: 1rem;">${isOnline ? '💬' : '📧'}</div>
        <h3 style="color: #1e3c72; margin-bottom: 1rem; font-size: 1.5rem;">
            ${isOnline ? 'Start Live Chat' : 'Leave a Message'}
        </h3>
        <p style="color: #666; margin-bottom: 2rem; line-height: 1.6;">
            ${message}
        </p>
        <div style="display: flex; gap: 1rem; justify-content: center;">
            <button onclick="proceedToChat(${isOnline})" style="
                background: linear-gradient(135deg, #28a745 0%, #20c997 100%);
                color: white;
                border: none;
                padding: 0.75rem 2rem;
                border-radius: 25px;
                cursor: pointer;
                font-weight: 600;
                transition: transform 0.2s ease;
            " onmouseover="this.style.transform='scale(1.05)'" onmouseout="this.style.transform='scale(1)'">
                ${isOnline ? 'Start Chat' : 'Leave Message'}
            </button>
            <button onclick="closeChatModal()" style="
                background: #6c757d;
                color: white;
                border: none;
                padding: 0.75rem 2rem;
                border-radius: 25px;
                cursor: pointer;
                font-weight: 600;
                transition: transform 0.2s ease;
            " onmouseover="this.style.transform='scale(1.05)'" onmouseout="this.style.transform='scale(1)'">
                Cancel
            </button>
        </div>
    `;
    
    overlay.appendChild(modal);
    document.body.appendChild(overlay);
    
    // Close modal when clicking overlay
    overlay.addEventListener('click', function(e) {
        if (e.target === overlay) {
            closeChatModal();
        }
    });
}

function proceedToChat(isOnline) {
    closeChatModal();
    
    if (isOnline) {
        // Replace with your actual chat integration
        // Examples:
        // - Intercom: window.Intercom('show');
        // - Zendesk: window.zE('webWidget', 'open');
        // - Custom chat: window.open('/chat', '_blank');
        
        // For demo purposes, redirect to a contact form or chat platform
        window.open('mailto:support@travelresorts.com?subject=Chat Support Request&body=Hi, I would like to chat with a support agent about my travel needs.', '_blank');
    } else {
        // Redirect to contact form for offline messages
        window.open('mailto:support@travelresorts.com?subject=Support Message&body=Hi, I am leaving this message during offline hours. Please get back to me within 24 hours regarding my travel inquiry.', '_blank');
    }
}

function closeChatModal() {
    const overlay = document.querySelector('div[style*="position: fixed"][style*="z-index: 10000"]');
    if (overlay) {
        overlay.remove();
    }
}

// Helper function to get current EST/EDT time
function getESTDateTime() {
    const now = new Date();
    const estTime = new Date(now.toLocaleString("en-US", {timeZone: "America/New_York"}));
    return {
        day: estTime.getDay(),
        hour: estTime.getHours(),
        minute: estTime.getMinutes(),
        fullDate: estTime
    };
}

// Update chat button status based on EST time
function updateChatStatus() {
    const chatStatus = document.querySelector('.chat-status');
    const { day, hour } = getESTDateTime();
    
    let isOnline = false;
    
    if (day >= 1 && day <= 5) { // Monday to Friday
        isOnline = hour >= 9 && hour < 17; // 9AM to 5PM EST
    }
    // Weekends are always offline
    
    if (chatStatus) {
        chatStatus.className = `chat-status ${isOnline ? 'online' : 'offline'}`;
        if (!isOnline) {
            chatStatus.style.backgroundColor = '#dc3545';
            chatStatus.style.animation = 'none';
        } else {
            chatStatus.style.backgroundColor = '#fff';
            chatStatus.style.animation = 'blink 1.5s infinite';
        }
    }
}

// Smooth scrolling for anchor links
function smoothScroll() {
    const links = document.querySelectorAll('a[href^="#"]');
    
    links.forEach(link => {
        link.addEventListener('click', function(e) {
            e.preventDefault();
            
            const targetId = this.getAttribute('href');
            const targetSection = document.querySelector(targetId);
            
            if (targetSection) {
                targetSection.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });
}

// Initialize page functionality
document.addEventListener('DOMContentLoaded', function() {
    updateChatStatus();
    smoothScroll();
    
    // Update chat status every minute
    setInterval(updateChatStatus, 60000);
    
    // Add scroll effect to header
    let lastScrollY = window.scrollY;
    
    window.addEventListener('scroll', function() {
        const header = document.querySelector('.header');
        
        if (window.scrollY > lastScrollY && window.scrollY > 100) {
            // Scrolling down
            header.style.transform = 'translateY(-100%)';
        } else {
            // Scrolling up
            header.style.transform = 'translateY(0)';
        }
        
        lastScrollY = window.scrollY;
    });
    
    // Add fade-in animation to sections
    const observerOptions = {
        threshold: 0.1,
        rootMargin: '0px 0px -50px 0px'
    };
    
    const observer = new IntersectionObserver(function(entries) {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.style.opacity = '1';
                entry.target.style.transform = 'translateY(0)';
            }
        });
    }, observerOptions);
    
    // Observe sections for animation
    const sections = document.querySelectorAll('.chat-hours, .services, .terms');
    sections.forEach(section => {
        section.style.opacity = '0';
        section.style.transform = 'translateY(30px)';
        section.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
        observer.observe(section);
    });
});

// Add CSS transitions to header
document.addEventListener('DOMContentLoaded', function() {
    const header = document.querySelector('.header');
    if (header) {
        header.style.transition = 'transform 0.3s ease';
    }
});