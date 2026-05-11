"""
Клавиатуры для управления нодами (Phase 4).
"""

from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from ui_constants import (
    CB_ADMIN_NODES,
    CB_ADMIN_NODES_MANAGE_PREFIX,
    CB_ADMIN_NODES_VISIBILITY_TOGGLE_PREFIX,
    CB_ADMIN_NODES_CAPACITY_EDIT_PREFIX,
    CB_ADMIN_BACK_MAIN,
)


def get_admin_nodes_list_kb(nodes: list[dict]) -> InlineKeyboardMarkup:
    """
    Клавиатура списка нод для админа.
    
    Args:
        nodes: Список нод из БД (get_all_nodes_for_admin)
    """
    keyboard: list[list[InlineKeyboardButton]] = []
    
    for node in nodes:
        node_id = node["id"]
        name = node["name"]
        status = node["status"]
        is_visible = node["is_visible"]
        active = node["active_configs"]
        capacity = node["capacity"]
        
        # Статус эмодзи
        status_emoji = "🟢" if status == "ready" else "🔴" if status in ("degraded", "offline") else "🟡"
        visibility_emoji = "👁️" if is_visible else "🙈"
        
        button_text = f"{status_emoji} {visibility_emoji} {name} ({active}/{capacity})"
        keyboard.append([
            InlineKeyboardButton(
                text=button_text,
                callback_data=f"{CB_ADMIN_NODES_MANAGE_PREFIX}{node_id}"
            )
        ])
    
    # Кнопка назад
    keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_admin_node_manage_kb(node: dict) -> InlineKeyboardMarkup:
    """
    Клавиатура управления отдельной нодой.
    
    Args:
        node: Данные ноды
    """
    node_id = node["id"]
    is_visible = node["is_visible"]
    capacity = node["capacity"]
    
    visibility_text = "🙈 Скрыть" if is_visible else "👁️ Показать"
    visibility_cb = f"{CB_ADMIN_NODES_VISIBILITY_TOGGLE_PREFIX}{node_id}"
    
    keyboard: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text=visibility_text, callback_data=visibility_cb)],
        [InlineKeyboardButton(text="✏️ Изменить capacity", callback_data=f"{CB_ADMIN_NODES_CAPACITY_EDIT_PREFIX}{node_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к списку", callback_data=CB_ADMIN_NODES)],
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_admin_node_capacity_edit_kb(node_id: int) -> InlineKeyboardMarkup:
    """
    Клавиатура для редактирования capacity ноды.
    """
    keyboard: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="➕ +5", callback_data=f"{CB_ADMIN_NODES_CAPACITY_EDIT_PREFIX}{node_id}:+5")],
        [InlineKeyboardButton(text="➕ +10", callback_data=f"{CB_ADMIN_NODES_CAPACITY_EDIT_PREFIX}{node_id}:+10")],
        [InlineKeyboardButton(text="➖ -5", callback_data=f"{CB_ADMIN_NODES_CAPACITY_EDIT_PREFIX}{node_id}:-5")],
        [InlineKeyboardButton(text="➖ -10", callback_data=f"{CB_ADMIN_NODES_CAPACITY_EDIT_PREFIX}{node_id}:-10")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{CB_ADMIN_NODES_MANAGE_PREFIX}{node_id}")],
    ]
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)
