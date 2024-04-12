import React from 'react';
import { Link, useNavigate, useLocation } from "react-router-dom";

const Sidebar = () => {
    const navigate = useNavigate();

    const loginUser = JSON.parse(localStorage.getItem('loggeduser'));

    const handleLogout = () => {
        localStorage.removeItem('loggeduser');
        navigate('/home');
    };

    const handleRedirectUser = () => {
        navigate('/mainuserpage');
    };

    const location = useLocation();

    function handleActive(path) {

        return location.pathname === path ? 'active' : '';
    }

    return (
        <nav className="sidebar">
            <div className="container-fluid justify-content-between">
                <div>

                    <ul className="loggedout nav d-inline-flex">
                        <li className="nav-item">
                            <Link to={"/home"} className={`nav-link ${handleActive('/home')}`}>
                                Home
                            </Link>
                        </li>
                        {loginUser && (
                            <>
                                <li className="nav-item">
                                    <Link to={"/classification"} className={`nav-link ${handleActive('/classification')}`}>
                                        Classification
                                    </Link>
                                </li>
                                <li className="nav-item">
                                    <Link to={"/processinghistory"} className={`nav-link ${handleActive('/processinghistory')}`}>
                                        History
                                    </Link>
                                </li>
                                <li className="nav-item">
                                    <Link to={"/payment"} className={`nav-link ${handleActive('/payment')}`}>
                                        Payment
                                    </Link>
                                </li>
                            </>
                        )}
                    </ul>
                </div>
                <ul className="navbar-nav">
                    {loginUser ? (
                        <>
                            <li className="nav-item">
                                <Link to={"/mainuserpage"} className={`nav-link userlog ${handleActive('/mainuserpage')}`}>
                                    User
                                </Link>
                            </li>

                            <li className="loggedlog nav-item">
                                <button onClick={handleLogout} className="nav-link">Logout</button>
                            </li>
                        </>
                    ) : (
                        <li className="loggedlog nav-item">
                            <Link to="/login" className="nav-link">Login</Link>
                        </li>
                    )}
                </ul>
            </div>
        </nav>
    );
}

export default Sidebar;