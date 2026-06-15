from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple


USER_ROLES = {"user", "human", "end_user"}
GPT_RESPONSE_ROLES = {"assistant", "model"}


def is_gpt_response_record(raw: Any) -> bool:
    if not isinstance(raw, dict):
        return False

    message = raw.get("message")
    if isinstance(message, dict):
        return _role_from(message) in GPT_RESPONSE_ROLES

    return _role_from(raw) in GPT_RESPONSE_ROLES


def extract_prompts(raw: Any, *, source_log_id: Optional[str]) -> List[Dict[str, Any]]:
    prompts: List[Dict[str, Any]] = []

    def walk(value: Any, context: Dict[str, Any]) -> None:
        if isinstance(value, dict):
            next_context = _merge_context(context, value)
            role = _role_from(value)
            content = _content_from(value)
            if role in USER_ROLES and content is not None:
                prompts.append(
                    {
                        "source_log_id": source_log_id,
                        "event_id": _first_value(value, ("event_id", "eventId")) or next_context.get("event_id"),
                        "conversation_id": _first_value(value, ("conversation_id", "conversationId"))
                        or next_context.get("conversation_id"),
                        "message_id": _first_value(value, ("message_id", "messageId", "id"))
                        or next_context.get("message_id"),
                        "user_id": _user_id_from(value) or next_context.get("user_id"),
                        "user_email": _user_email_from(value) or next_context.get("user_email"),
                        "created_at": _first_value(
                            value,
                            ("created_at", "createdAt", "create_time", "createTime", "timestamp", "time", "created"),
                        )
                        or next_context.get("created_at"),
                        "role": "user",
                        "content": content,
                        "raw": value,
                    }
                )
            for child in value.values():
                walk(child, next_context)
            return

        if isinstance(value, list):
            for item in value:
                walk(item, context)

    walk(raw, {})
    return _deduplicate_prompts(prompts)


def _merge_context(context: Dict[str, Any], value: Dict[str, Any]) -> Dict[str, Any]:
    next_context = dict(context)
    updates = {
        "event_id": _first_value(value, ("event_id", "eventId")),
        "conversation_id": _conversation_id_from(value),
        "message_id": _first_value(value, ("message_id", "messageId")),
        "user_id": _user_id_from(value),
        "user_email": _user_email_from(value),
        "created_at": _first_value(
            value,
            ("created_at", "createdAt", "create_time", "createTime", "timestamp", "time", "created"),
        ),
    }
    for key, item in updates.items():
        if item is not None and key not in next_context:
            next_context[key] = item
    return next_context


def _role_from(value: Dict[str, Any]) -> Optional[str]:
    role = value.get("role") or value.get("sender_role") or value.get("senderRole")
    if role is None and isinstance(value.get("author"), dict):
        role = value["author"].get("role") or value["author"].get("type")
    if role is None and isinstance(value.get("sender"), dict):
        role = value["sender"].get("role") or value["sender"].get("type")
    if isinstance(role, str):
        return role.lower()
    return None


def _content_from(value: Dict[str, Any]) -> Any:
    if "content" in value:
        return _normalize_content(value["content"])
    for key in ("text", "prompt", "input"):
        if key in value:
            return _normalize_content(value[key])
    if isinstance(value.get("message"), str):
        return value["message"]
    return None


def _normalize_content(content: Any) -> Any:
    if isinstance(content, dict):
        if "parts" in content:
            parts = content["parts"]
            if isinstance(parts, list) and len(parts) == 1:
                return parts[0]
            return parts
        if "text" in content:
            return content["text"]
        if "value" in content:
            return content["value"]
    return content


def _conversation_id_from(value: Dict[str, Any]) -> Any:
    found = _first_value(value, ("conversation_id", "conversationId"))
    if found is not None:
        return found
    conversation = value.get("conversation")
    if isinstance(conversation, dict):
        return _first_value(conversation, ("id", "conversation_id", "conversationId"))
    if isinstance(conversation, str):
        return conversation
    return None


def _user_id_from(value: Dict[str, Any]) -> Any:
    found = _first_value(value, ("user_id", "userId", "actor_id", "actorId", "account_id", "accountId"))
    if found is not None:
        return found
    for key in ("user", "actor", "author", "sender"):
        nested = value.get(key)
        if isinstance(nested, dict):
            nested_id = _first_value(nested, ("id", "user_id", "userId", "email"))
            if nested_id is not None:
                return nested_id
    return None


def _user_email_from(value: Dict[str, Any]) -> Any:
    found = _first_value(value, ("user_email", "userEmail", "email", "email_address", "emailAddress"))
    if found is not None:
        return found
    for key in ("user", "actor", "author", "sender"):
        nested = value.get(key)
        if isinstance(nested, dict):
            nested_email = _first_value(nested, ("user_email", "userEmail", "email", "email_address", "emailAddress"))
            if nested_email is not None:
                return nested_email
    return None


def _first_value(value: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in value and value[key] is not None:
            return value[key]
    return None


def _deduplicate_prompts(prompts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[Tuple[Any, Any, str]] = set()
    deduped: List[Dict[str, Any]] = []
    for prompt in prompts:
        if prompt.get("message_id") is None:
            deduped.append(prompt)
            continue
        marker = (
            prompt.get("source_log_id"),
            prompt.get("message_id"),
            repr(prompt.get("content")),
        )
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(prompt)
    return deduped
