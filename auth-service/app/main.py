import logging
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stderr,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

import streamlit as st
from database.connection import init_db
from auth.authentication import (
    login_user,
    register_user,
    get_user_license_keys,
    is_valid_email,
    is_strong_password,
)
from auth.two_factor import (
    send_login_challenge,
    verify_2fa_code,
    get_user_2fa_methods,
    initiate_2fa_setup,
    confirm_2fa_setup,
    disable_2fa_method,
)
from utils.validators import is_valid_phone, mask_contact

st.set_page_config(
    page_title="Auth Service",
    page_icon="🔐",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ── Minimal styling ──────────────────────────────────────────────────────────
st.markdown(
    """
<style>
.main .block-container { max-width: 480px; padding-top: 3rem; }
.auth-title { font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem; }
.auth-subtitle { color: #888; margin-bottom: 2rem; }
.key-box {
    background: #1a1a1a; border: 1px solid #333; border-radius: 8px;
    padding: 1rem; font-family: monospace; font-size: 0.9rem;
    word-break: break-all; margin: 0.5rem 0;
}
.badge-ok { color: #22c55e; font-weight: 600; }
.badge-warn { color: #f59e0b; font-weight: 600; }
</style>
""",
    unsafe_allow_html=True,
)


# ── Session state helpers ────────────────────────────────────────────────────
def _init_session():
    defaults = {
        "page": "login",  # login | register | 2fa_challenge | dashboard
        "user": None,  # set after password auth
        "password_ok": False,
        "fully_authenticated": False,
        "pending_method": None,  # method used for login 2FA challenge
        "setup_step": None,  # None | 'enter_contact' | 'verify_code'
        "setup_type": None,  # 'email' | 'sms'
        "setup_contact": None,
        "flash_ok": None,
        "flash_err": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def _flash(msg: str, ok: bool = True):
    if ok:
        st.session_state.flash_ok = msg
    else:
        st.session_state.flash_err = msg


def _show_flash():
    if st.session_state.flash_ok:
        st.success(st.session_state.flash_ok)
        st.session_state.flash_ok = None
    if st.session_state.flash_err:
        st.error(st.session_state.flash_err)
        st.session_state.flash_err = None


def _logout():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()


# ── Pages ────────────────────────────────────────────────────────────────────
def page_login():
    st.markdown('<p class="auth-title">🔐 Sign In</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="auth-subtitle">Enter your credentials to continue.</p>',
        unsafe_allow_html=True,
    )
    _show_flash()

    with st.form("login_form"):
        email = st.text_input("Email address", placeholder="you@example.com")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign In", use_container_width=True)

    if submitted:
        if not email or not password:
            st.error("Please fill in all fields.")
        else:
            ok, msg, user_data = login_user(email, password)
            if not ok or not user_data:
                st.error(msg)
            else:
                st.session_state.user = user_data
                st.session_state.password_ok = True

                if user_data["has_2fa"]:
                    # Pick first available method to send challenge
                    method = user_data["2fa_methods"][0]
                    st.session_state.pending_method = method
                    sent_ok, sent_msg = send_login_challenge(user_data["id"], method)
                    if not sent_ok:
                        st.error(f"Could not send 2FA code: {sent_msg}")
                        st.session_state.password_ok = False
                        st.session_state.user = None
                    else:
                        st.session_state.page = "2fa_challenge"
                        st.rerun()
                else:
                    # No 2FA yet — let them in but restrict license key
                    st.session_state.fully_authenticated = True
                    st.session_state.page = "dashboard"
                    st.rerun()

    st.markdown("---")
    if st.button("Create an account", use_container_width=True):
        st.session_state.page = "register"
        st.rerun()


def page_register():
    st.markdown('<p class="auth-title">🔐 Create Account</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="auth-subtitle">Password must be 8+ characters with uppercase, lowercase, number, and special character.</p>',
        unsafe_allow_html=True,
    )
    _show_flash()

    with st.form("register_form"):
        email = st.text_input("Email address", placeholder="you@example.com")
        password = st.text_input("Password", type="password")
        confirm = st.text_input("Confirm password", type="password")
        submitted = st.form_submit_button("Create Account", use_container_width=True)

    if submitted:
        if not email or not password or not confirm:
            st.error("Please fill in all fields.")
        elif password != confirm:
            st.error("Passwords do not match.")
        else:
            ok, msg = register_user(email, password)
            if not ok:
                st.error(msg)
            else:
                _flash("Account created. Please sign in.")
                st.session_state.page = "login"
                st.rerun()

    st.markdown("---")
    if st.button("Already have an account? Sign in", use_container_width=True):
        st.session_state.page = "login"
        st.rerun()


def page_2fa_challenge():
    user = st.session_state.user
    method = st.session_state.pending_method

    st.markdown('<p class="auth-title">🔐 Verify Identity</p>', unsafe_allow_html=True)
    masked = mask_contact(method["contact"], method["type"])
    label = "email" if method["type"] == "email" else "SMS"
    st.markdown(
        f'<p class="auth-subtitle">A 6-digit code was sent via {label} to <strong>{masked}</strong>.</p>',
        unsafe_allow_html=True,
    )
    _show_flash()

    with st.form("2fa_form"):
        code = st.text_input("Verification code", placeholder="000000", max_chars=6)
        submitted = st.form_submit_button("Verify", use_container_width=True)

    if submitted:
        if not code:
            st.error("Please enter the code.")
            return
        ok, msg = verify_2fa_code(user["id"], code)
        if not ok:
            st.error(msg)
            return
        st.session_state.fully_authenticated = True
        st.session_state.page = "dashboard"
        st.rerun()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Resend code", use_container_width=True):
            sent_ok, sent_msg = send_login_challenge(user["id"], method)
            if sent_ok:
                _flash("Code resent.")
            else:
                _flash(sent_msg, ok=False)
            st.rerun()

    # If user has multiple 2FA methods let them switch
    other_methods = [m for m in user["2fa_methods"] if m["id"] != method["id"]]
    if other_methods:
        with col2:
            if st.button("Use different method", use_container_width=True):
                new_method = other_methods[0]
                st.session_state.pending_method = new_method
                sent_ok, sent_msg = send_login_challenge(user["id"], new_method)
                if not sent_ok:
                    _flash(sent_msg, ok=False)
                st.rerun()

    st.markdown("---")
    if st.button("Cancel — back to login", use_container_width=True):
        _logout()


def page_dashboard():
    user = st.session_state.user
    fully_auth = st.session_state.fully_authenticated

    # Top bar
    col_title, col_logout = st.columns([4, 1])
    with col_title:
        st.markdown(
            f'<p class="auth-title">👤 {user["email"]}</p>', unsafe_allow_html=True
        )
    with col_logout:
        if st.button("Log out", use_container_width=True):
            _logout()

    _show_flash()

    # ── 2FA status banner ────────────────────────────────────────────────────
    methods = get_user_2fa_methods(user["id"])
    active_methods = [m for m in methods if m["verified"] and m["enabled"]]

    if not fully_auth:
        st.warning(
            "You are signed in with password only. "
            "Set up 2FA below to access your license key.",
            icon="⚠️",
        )
    elif not active_methods:
        st.warning(
            "No verified 2FA methods. Add one below to unlock your license key.",
            icon="⚠️",
        )

    st.markdown("---")

    # ── License key section ──────────────────────────────────────────────────
    st.subheader("License Key")

    if not active_methods:
        st.info(
            "A verified 2FA method is required to view your license key.",
            icon="🔒",
        )
    else:
        license_keys = get_user_license_keys(user["id"])
        if not license_keys:
            st.info("No license key assigned to your account yet.", icon="ℹ️")
        else:
            for lk in license_keys:
                st.markdown(
                    f'<div class="key-box">{lk["key"]}</div>', unsafe_allow_html=True
                )
                expiry = lk["expires_at"]
                if expiry:
                    st.caption(f"Expires: {expiry.strftime('%Y-%m-%d')}")
                else:
                    st.caption("No expiry")

    st.markdown("---")

    # ── 2FA management ───────────────────────────────────────────────────────
    st.subheader("Two-Factor Authentication")

    for m in methods:
        col_info, col_action = st.columns([3, 1])
        with col_info:
            icon = "✅" if m["enabled"] and m["verified"] else "⚙️"
            label = "Email" if m["type"] == "email" else "SMS"
            status = "Active" if m["enabled"] and m["verified"] else "Not verified"
            st.markdown(
                f"{icon} **{label}** — {mask_contact(m['contact'], m['type'])}  \n"
                f"<small style='color:#888'>{status}</small>",
                unsafe_allow_html=True,
            )
        with col_action:
            if m["enabled"] and m["verified"]:
                if st.button("Disable", key=f"disable_{m['type']}", use_container_width=True):
                    ok, msg = disable_2fa_method(user["id"], m["type"])
                    if ok:
                        _flash(msg)
                    else:
                        _flash(msg, ok=False)
                    st.rerun()

    st.markdown("")
    col_add_email, col_add_sms = st.columns(2)
    with col_add_email:
        email_method = next((m for m in methods if m["type"] == "email"), None)
        btn_label = "Re-setup Email 2FA" if email_method else "Add Email 2FA"
        if st.button(btn_label, use_container_width=True):
            st.session_state.setup_type = "email"
            st.session_state.setup_step = "enter_contact"
            st.session_state.setup_contact = None
            st.rerun()

    with col_add_sms:
        sms_method = next((m for m in methods if m["type"] == "sms"), None)
        btn_label = "Re-setup SMS 2FA" if sms_method else "Add SMS 2FA"
        if st.button(btn_label, use_container_width=True):
            st.session_state.setup_type = "sms"
            st.session_state.setup_step = "enter_contact"
            st.session_state.setup_contact = None
            st.rerun()

    # ── 2FA setup inline flow ────────────────────────────────────────────────
    if st.session_state.setup_step == "enter_contact":
        _section_setup_enter_contact()
    elif st.session_state.setup_step == "verify_code":
        _section_setup_verify_code()


def _section_setup_enter_contact():
    method_type = st.session_state.setup_type
    st.markdown("---")
    st.subheader(f"Set up {'Email' if method_type == 'email' else 'SMS'} 2FA")

    if method_type == "email":
        with st.form("setup_email_form"):
            contact = st.text_input(
                "Email address for 2FA codes",
                placeholder="you@example.com",
            )
            submitted = st.form_submit_button("Send code", use_container_width=True)

        if submitted:
            if not is_valid_email(contact):
                st.error("Enter a valid email address.")
                return
            ok, msg = initiate_2fa_setup(
                st.session_state.user["id"], "email", contact.strip().lower()
            )
            if not ok:
                st.error(msg)
                return
            st.session_state.setup_contact = contact.strip().lower()
            st.session_state.setup_step = "verify_code"
            _flash("Code sent. Enter it below.")
            st.rerun()
    else:
        with st.form("setup_sms_form"):
            phone_raw = st.text_input(
                "US mobile number",
                placeholder="555-867-5309",
            )
            submitted = st.form_submit_button("Send code", use_container_width=True)

        if submitted:
            valid, phone_e164 = is_valid_phone(phone_raw)
            if not valid:
                st.error(phone_e164)
                return
            ok, msg = initiate_2fa_setup(
                st.session_state.user["id"], "sms", phone_e164
            )
            if not ok:
                st.error(msg)
                return
            st.session_state.setup_contact = phone_e164
            st.session_state.setup_step = "verify_code"
            _flash("Code sent. Enter it below.")
            st.rerun()

    if st.button("Cancel", use_container_width=True):
        st.session_state.setup_step = None
        st.session_state.setup_type = None
        st.rerun()


def _section_setup_verify_code():
    method_type = st.session_state.setup_type
    contact = st.session_state.setup_contact
    user_id = st.session_state.user["id"]

    st.markdown("---")
    masked = mask_contact(contact, method_type)
    st.subheader("Enter verification code")
    st.caption(f"Code sent to {masked}")
    _show_flash()

    with st.form("setup_verify_form"):
        code = st.text_input("6-digit code", placeholder="000000", max_chars=6)
        submitted = st.form_submit_button("Verify & Enable", use_container_width=True)

    if submitted:
        if not code:
            st.error("Enter the code.")
            return
        ok, msg = confirm_2fa_setup(user_id, method_type, code)
        if not ok:
            st.error(msg)
            return

        # Refresh user 2FA state
        st.session_state.fully_authenticated = True
        st.session_state.setup_step = None
        st.session_state.setup_type = None
        st.session_state.setup_contact = None
        _flash(msg)
        st.rerun()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Resend code", use_container_width=True):
            ok, msg = initiate_2fa_setup(user_id, method_type, contact)
            if ok:
                _flash("Code resent.")
            else:
                _flash(msg, ok=False)
            st.rerun()
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.session_state.setup_step = None
            st.session_state.setup_type = None
            st.rerun()


# ── Entry point ──────────────────────────────────────────────────────────────
def main():
    _init_session()
    init_db()

    page = st.session_state.page

    if not st.session_state.password_ok:
        if page == "register":
            page_register()
        else:
            page_login()
    elif page == "2fa_challenge" and not st.session_state.fully_authenticated:
        page_2fa_challenge()
    else:
        page_dashboard()


if __name__ == "__main__":
    main()
